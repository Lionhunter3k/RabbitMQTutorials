using Domain;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FaultTolerantMessaging
{
    class Program
    {
        const string retryHeaderName = "number-of-retries";

        const string fromQueueHeaderName = "from-queue";

        const int maxNumberOfRetries = 2;

        static ReaderWriterLockSlim readerWriterLock = new ReaderWriterLockSlim();

        static async Task Main(string[] args)
        {
            ConnectionFactory connectionFactory = new ConnectionFactory
            {
                Port = 5672,
                HostName = "localhost",
                UserName = "accountant",
                Password = "accountant",
                VirtualHost = "accounting"
            };
            try
            {
                using (IConnection connection = connectionFactory.CreateConnection())
                {
                    SetupExchange(connection, "my.error.queue");
                    SetupExchange(connection, "my.second.queue", "my.first.exchange");
                    var producerPromises = new List<Task>();
                    var producerTokenSource = new CancellationTokenSource();
                    for (var i = 0; i < 4; i++)
                    {
                        producerPromises.Add(Task.Run(() =>
                        {
                            Console.WriteLine("Producer has started");
                            {
                                for (var j = 0; i < int.MaxValue; i++)
                                    SendExchange(connection, "my.first.exchange", string.Empty);
                            }
                            Console.WriteLine("Producer has stopped");
                        }, producerTokenSource.Token));
                    }
                    var channelCompletionDeffered = new TaskCompletionSource<object>();
                    var workerPromise = Task.Run(async () =>
                    {
                        using (var channels = new DisposableLinkedList<IModel>())
                        {
                            for (var i = 0; i < 4; i++)
                            {
                                channels.AddLast(ReceiveMessagesWithEvents(connection, "my.second.queue", false, "my.error.queue"));
                            }
                            await channelCompletionDeffered.Task;
                        }
                    });
                    Console.WriteLine("Press any key to stop consuming");
                    Console.ReadKey();
                    readerWriterLock.EnterWriteLock();
                    channelCompletionDeffered.SetResult(null);
                    producerTokenSource.Cancel();
                    await Task.WhenAll(workerPromise);
                }
            }
            finally
            {
                if (readerWriterLock.IsWriteLockHeld)
                    readerWriterLock.ExitWriteLock();
            }
        }

        private static void SendExchange(IConnection connection, string destinationExchange, string routingKey)
        {
            readerWriterLock.EnterReadLock();
            try
            {
                if(connection.IsOpen)
                {
                    using (IModel channel = connection.CreateModel())
                    {
                        channel.ConfirmSelect();
                        channel.BasicAcks += Channel_BasicAcks;
                        channel.BasicNacks += Channel_BasicNacks;
                        channel.BasicReturn += Channel_BasicReturn;

                        IBasicProperties properties = channel.CreateBasicProperties();
                        channel.BasicPublish(exchange: destinationExchange, mandatory: true, routingKey: routingKey, basicProperties: properties, body: Encoding.UTF8.GetBytes("This is a message from the RabbitMq .NET driver"));
                    }
                }
            }
            finally
            {
                readerWriterLock.ExitReadLock();
            }
        }

        private static string SetupExchange(IConnection connection, string inputQueue, string destinationExchange = null, string exchangeType = "direct", string routingKey = "")
        {
            using (IModel channel = connection.CreateModel())
            {
                var queueResult = channel.QueueDeclare(queue: inputQueue, durable: true, exclusive: false, autoDelete: false, arguments: null);
                if(destinationExchange != null)
                {
                    channel.ExchangeDeclare(exchange: destinationExchange, type: exchangeType, durable: true, autoDelete: false, arguments: null);
                    channel.QueueBind(queue: inputQueue, exchange: destinationExchange, routingKey: routingKey);
                }
                return queueResult.QueueName;
            }
        }

        private static IModel ReceiveMessagesWithEvents(IConnection connection, string outputQueue, bool shouldAck, string errorQueue)
        {
            IModel channel = connection.CreateModel();
            channel.BasicQos(0, 1, false);
            EventingBasicConsumer eventingBasicConsumer = new EventingBasicConsumer(channel);

            eventingBasicConsumer.Received += (sender, basicDeliveryEventArgs) =>
            {
                readerWriterLock.EnterReadLock();
                try
                {
                    if(connection.IsOpen)
                    {
                        IBasicProperties basicProperties = basicDeliveryEventArgs.BasicProperties;
                        Debug.WriteLine(string.Concat("Message received from the exchange ", basicDeliveryEventArgs.Exchange));
                        Debug.WriteLine(string.Concat("Routing key: ", basicDeliveryEventArgs.RoutingKey));
                        Debug.WriteLine(string.Concat("Message: ", Encoding.UTF8.GetString(basicDeliveryEventArgs.Body)));
                        if (shouldAck)
                            channel.BasicAck(deliveryTag: basicDeliveryEventArgs.DeliveryTag, multiple: false);
                        else
                        {
                            int retryCount = GetRetryCount(basicProperties);
                            channel.BasicNack(deliveryTag: basicDeliveryEventArgs.DeliveryTag, multiple: false, requeue: false);
                            if (retryCount < maxNumberOfRetries)
                            {
                                IBasicProperties propertiesForCopy = channel.CreateBasicProperties();
                                propertiesForCopy.Headers = CopyHeaders(basicProperties);
                                propertiesForCopy.Headers[retryHeaderName] = ++retryCount;
                                channel.BasicPublish(basicDeliveryEventArgs.Exchange, basicDeliveryEventArgs.RoutingKey, propertiesForCopy, basicDeliveryEventArgs.Body);
                            }
                            else
                            {
                                basicProperties.Headers.Add(fromQueueHeaderName, outputQueue);
                                channel.BasicPublish(string.Empty, errorQueue, basicProperties, basicDeliveryEventArgs.Body);
                            }
                        }
                    }
                }
                finally
                {
                    readerWriterLock.ExitReadLock();
                }
            };

            channel.BasicConsume(queue: outputQueue, autoAck: false, consumer: eventingBasicConsumer);

            return channel;
        }

        private static IDictionary<string, object> CopyHeaders(IBasicProperties originalProperties)
        {
            IDictionary<string, object> dict = new Dictionary<string, object>();
            IDictionary<string, object> headers = originalProperties.Headers;
            if (headers != null)
            {
                foreach (KeyValuePair<string, object> kvp in headers)
                {
                    dict[kvp.Key] = kvp.Value;
                }
            }

            return dict;
        }

        private static int GetRetryCount(IBasicProperties messageProperties)
        {
            IDictionary<string, object> headers = messageProperties.Headers;
            int count = 0;
            if (headers != null)
            {
                if (headers.TryGetValue(retryHeaderName, out var countHeader))
                {
                    if (countHeader is byte[] countHeaderBody)
                        count = BitConverter.ToInt32(countHeaderBody, 0);
                    else
                        if (countHeader is int countHeaderValue)
                            count = countHeaderValue;
                }
                else
                {
                    headers.Add(retryHeaderName, 0);
                }
            }
            else
                messageProperties.Headers = new Dictionary<string, object>();
            return count;
        }

        private static void Channel_BasicNacks(object sender, BasicNackEventArgs e)
        {
            Console.WriteLine(string.Concat("Message broker could not acknowledge message with tag: ", e.DeliveryTag));
        }

        private static void Channel_BasicAcks(object sender, BasicAckEventArgs e)
        {
            Console.WriteLine(string.Concat("Message broker has acknowledged message with tag: ", e.DeliveryTag));
        }

        private static void Channel_BasicReturn(object sender, BasicReturnEventArgs e)
        {
            Console.WriteLine(string.Concat("Queue is missing for the message: ", Encoding.UTF8.GetString(e.Body)));
            Console.WriteLine(string.Concat("Reply code and text: ", e.ReplyCode, " ", e.ReplyText));
        }
    }
}
