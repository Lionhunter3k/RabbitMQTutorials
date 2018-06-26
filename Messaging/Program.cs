using Domain;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Messaging
{
    class Program
    {
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
                    var producerPromises = new List<Task>();
                    var producerTokenSource = new CancellationTokenSource();
                    for (var i = 0; i < 4; i++)
                    {
                        producerPromises.Add(Task.Run(() =>
                        {
                            Console.WriteLine("Producer has started");
                            {
                                for (var j = 0; i < int.MaxValue; i++)
                                    SendDirectExchangeWithRoutingKey(connection);
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
                                channels.AddLast(ReceiveDirectMessagesWithEvents(connection));
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
                if(readerWriterLock.IsWriteLockHeld)
                    readerWriterLock.ExitWriteLock();
            }
        }

        private static void SendHeadersExchange(IConnection connection)
        {
            readerWriterLock.EnterReadLock();
            try
            {
                if (connection.IsOpen)
                {
                    using (IModel channel = connection.CreateModel())
                    {
                        channel.ExchangeDeclare("company.exchange.headers", ExchangeType.Headers, true, false, null);
                        channel.QueueDeclare("company.queue.headers", true, false, false, null);
                        Dictionary<string, object> headerOptionsWithAll = new Dictionary<string, object>
                {
                    { "x-match", "all" },
                    { "category", "animal" },
                    { "type", "mammal" }
                };

                        channel.QueueBind(queue: "company.queue.headers", exchange: "company.exchange.headers", routingKey: "", arguments: headerOptionsWithAll);

                        Dictionary<string, object> headerOptionsWithAny = new Dictionary<string, object>
                {
                    { "x-match", "any" },
                    { "category", "plant" },
                    { "type", "tree" }
                };

                        channel.QueueBind("company.queue.headers", "company.exchange.headers", "", headerOptionsWithAny);

                        PublicationAddress address = new PublicationAddress(exchangeType: ExchangeType.Headers, exchangeName: "company.exchange.headers", routingKey: "");

                        IBasicProperties properties = channel.CreateBasicProperties();
                        Dictionary<string, object> messageHeaders = new Dictionary<string, object>
                {
                    { "category", "animal" },
                    { "type", "insect" }
                };
                        properties.Headers = messageHeaders;
                        channel.BasicPublish(addr: address, basicProperties: properties, body: Encoding.UTF8.GetBytes("Hello from the world of insects"));

                        properties = channel.CreateBasicProperties();
                        messageHeaders = new Dictionary<string, object>
                {
                    { "category", "animal" },
                    { "type", "mammal" },
                    { "mood", "awesome" }
                };
                        properties.Headers = messageHeaders;
                        channel.BasicPublish(addr: address, basicProperties: properties, body: Encoding.UTF8.GetBytes("Hello from the world of awesome mammals"));

                        properties = channel.CreateBasicProperties();
                        messageHeaders = new Dictionary<string, object>
                {
                    { "category", "animal" },
                    { "type", "mammal" }
                };
                        properties.Headers = messageHeaders;
                        channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello from the world of mammals"));

                        properties = channel.CreateBasicProperties();
                        messageHeaders = new Dictionary<string, object>
                {
                    { "category", "animal" }
                };
                        properties.Headers = messageHeaders;
                        channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello from the world of animals"));

                        properties = channel.CreateBasicProperties();
                        messageHeaders = new Dictionary<string, object>
                {
                    { "category", "fungi" },
                    { "type", "champignon" }
                };
                        properties.Headers = messageHeaders;
                        channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello from the world of fungi"));

                        properties = channel.CreateBasicProperties();
                        messageHeaders = new Dictionary<string, object>
                {
                    { "category", "plant" }
                };
                        properties.Headers = messageHeaders;
                        channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello from the world of plants"));

                        properties = channel.CreateBasicProperties();
                        messageHeaders = new Dictionary<string, object>
                {
                    { "category", "plant" },
                    { "type", "tree" }
                };
                        properties.Headers = messageHeaders;
                        channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello from the world of trees"));

                        properties = channel.CreateBasicProperties();
                        messageHeaders = new Dictionary<string, object>
                {
                    { "mood", "sad" },
                    { "type", "tree" }
                };
                        properties.Headers = messageHeaders;
                        channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello from the world of sad trees"));
                    }
                }
            }
            finally
            {
                readerWriterLock.ExitReadLock();
            }
        }

        private static IModel ReceiveHeaderMessagesWithEvents(IConnection connection, bool shouldAck)
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
                        Debug.WriteLine(string.Concat("Message: ", Encoding.UTF8.GetString(basicDeliveryEventArgs.Body)));
                        StringBuilder headersBuilder = new StringBuilder();
                        headersBuilder.Append("Headers: ").Append(Environment.NewLine);
                        foreach (var kvp in basicProperties.Headers)
                        {
                            headersBuilder.Append(kvp.Key).Append(": ").Append(Encoding.UTF8.GetString(kvp.Value as byte[])).Append(Environment.NewLine);
                        }
                        Debug.WriteLine(headersBuilder.ToString());
                        if (shouldAck)
                            channel.BasicAck(deliveryTag: basicDeliveryEventArgs.DeliveryTag, multiple: false);
                        else
                            channel.BasicNack(basicDeliveryEventArgs.DeliveryTag, false, true);
                    }
                }
                finally
                {
                    readerWriterLock.ExitReadLock();
                }
            };

            channel.BasicConsume(queue: "company.queue.headers", autoAck: false, consumer: eventingBasicConsumer);

            return channel;
        }

        private static void SendTopicsExchange(IConnection connection)
        {
            readerWriterLock.EnterReadLock();
            try
            {
                if(connection.IsOpen)
                {
                    using (IModel channel = connection.CreateModel())
                    {
                        channel.ExchangeDeclare(exchange: "company.exchange.topic", type: ExchangeType.Topic, durable: true, autoDelete: false, arguments: null);
                        channel.QueueDeclare(queue: "company.queue.topic", durable: true, exclusive: false, autoDelete: false, arguments: null);
                        channel.QueueBind(queue: "company.queue.topic", exchange: "company.exchange.topic", routingKey: "*.world");
                        channel.QueueBind(queue: "company.queue.topic", exchange: "company.exchange.topic", routingKey: "world.#");

                        IBasicProperties properties = channel.CreateBasicProperties();
                        properties.Persistent = true;
                        properties.ContentType = "text/plain";
                        PublicationAddress address = new PublicationAddress(exchangeType: ExchangeType.Topic, exchangeName: "company.exchange.topic", routingKey: "news of the world");
                        channel.BasicPublish(addr: address, basicProperties: properties, body: Encoding.UTF8.GetBytes("This is some random news from the world"));

                        address = new PublicationAddress(exchangeType: ExchangeType.Topic, exchangeName: "company.exchange.topic", routingKey: "news.of.the.world");
                        channel.BasicPublish(address, basicProperties: properties, body: Encoding.UTF8.GetBytes("trololo"));

                        address = new PublicationAddress(exchangeType: ExchangeType.Topic, exchangeName: "company.exchange.topic", routingKey: "the world is crumbling");
                        channel.BasicPublish(address, basicProperties: properties, body: Encoding.UTF8.GetBytes("whatever"));

                        address = new PublicationAddress(exchangeType: ExchangeType.Topic, exchangeName: "company.exchange.topic", routingKey: "the.world.is.crumbling");
                        channel.BasicPublish(addr: address, basicProperties: properties, body: Encoding.UTF8.GetBytes("Hello"));

                        address = new PublicationAddress(exchangeType: ExchangeType.Topic, exchangeName: "company.exchange.topic", routingKey: "world.news.and.more");
                        channel.BasicPublish(address, basicProperties: properties, body: Encoding.UTF8.GetBytes("It's Friday night"));

                        address = new PublicationAddress(exchangeType: ExchangeType.Topic, exchangeName: "company.exchange.topic", routingKey: "world news and more");
                        channel.BasicPublish(addr: address, basicProperties: properties, body: Encoding.UTF8.GetBytes("No more tears"));

                        address = new PublicationAddress(exchangeType: ExchangeType.Topic, exchangeName: "company.exchange.topic", routingKey: "beautiful.world");
                        channel.BasicPublish(addr: address, basicProperties: properties, body: Encoding.UTF8.GetBytes("The world is beautiful"));
                    }
                }
            }
            finally
            {
                readerWriterLock.ExitReadLock();
            }
        }
        private static IModel ReceiveTopicMessagesWithEvents(IConnection connection)
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
                        channel.BasicAck(basicDeliveryEventArgs.DeliveryTag, false);
                    }
                }
                finally
                {
                    readerWriterLock.ExitReadLock();
                }
            };

            channel.BasicConsume(queue: "company.queue.topic", autoAck: false, consumer: eventingBasicConsumer);

            return channel;
        }


        private static void SendDirectExchangeWithRoutingKey(IConnection connection)
        {
            readerWriterLock.EnterReadLock();
            try
            {
                if(connection.IsOpen)
                {
                    using (IModel channel = connection.CreateModel())
                    {
                        channel.ExchangeDeclare(exchange: "company.exchange.routing", type: ExchangeType.Direct, durable: true, autoDelete: false, arguments: null);
                        channel.QueueDeclare(queue: "company.exchange.queue", durable: true, exclusive: false, autoDelete: false, arguments: null);
                        channel.QueueBind(queue: "company.exchange.queue", exchange: "company.exchange.routing", routingKey: "asia");
                        channel.QueueBind(queue: "company.exchange.queue", exchange: "company.exchange.routing", routingKey: "americas");
                        channel.QueueBind(queue: "company.exchange.queue", exchange: "company.exchange.routing", routingKey: "europe");

                        IBasicProperties properties = channel.CreateBasicProperties();
                        properties.Persistent = true;
                        properties.ContentType = "text/plain";
                        PublicationAddress address = new PublicationAddress(exchangeType: ExchangeType.Direct, exchangeName: "company.exchange.routing", routingKey: "asia");
                        channel.BasicPublish(addr: address, basicProperties: properties, body: Encoding.UTF8.GetBytes("The latest news from Asia!"));

                        address = new PublicationAddress(exchangeType: ExchangeType.Direct, exchangeName: "company.exchange.routing", routingKey: "europe");
                        channel.BasicPublish(addr: address, basicProperties: properties, body: Encoding.UTF8.GetBytes("The latest news from Europe!"));

                        address = new PublicationAddress(exchangeType: ExchangeType.Direct, exchangeName: "company.exchange.routing", routingKey: "americas");
                        channel.BasicPublish(addr: address, basicProperties: properties, body: Encoding.UTF8.GetBytes("The latest news from the Americas!"));

                        address = new PublicationAddress(exchangeType: ExchangeType.Direct, exchangeName: "company.exchange.routing", routingKey: "africa");
                        channel.BasicPublish(addr: address, basicProperties: properties, body: Encoding.UTF8.GetBytes("The latest news from Africa!"));

                        address = new PublicationAddress(exchangeType: ExchangeType.Direct, exchangeName: "company.exchange.routing", routingKey: "australia");
                        channel.BasicPublish(addr: address, basicProperties: properties, body: Encoding.UTF8.GetBytes("The latest news from Australia!"));
                    }
                }
            }
            finally
            {
                readerWriterLock.ExitReadLock();
            }
        }

        private static IModel ReceiveDirectMessagesWithEvents(IConnection connection)
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
                        channel.BasicAck(basicDeliveryEventArgs.DeliveryTag, false);
                    }
                }
                finally
                {
                    readerWriterLock.ExitReadLock();
                }
            };

            channel.BasicConsume(queue: "company.exchange.queue", autoAck: false, consumer: eventingBasicConsumer);

            return channel;
        }

        private static void SendFanoutExchange(IConnection connection)
        {
            readerWriterLock.EnterReadLock();
            try
            {
                if(connection.IsOpen)
                {
                    using (IModel channel = connection.CreateModel())
                    {
                        channel.ExchangeDeclare(exchange: "mycompany.fanout.exchange", type: ExchangeType.Fanout, durable: true, autoDelete: false, arguments: null);
                        channel.QueueDeclare(queue: "mycompany.queues.accounting", durable: true, exclusive: false, autoDelete: false, arguments: null);
                        channel.QueueDeclare(queue: "mycompany.queues.management", durable: true, exclusive: false, autoDelete: false, arguments: null);
                        channel.QueueBind(queue: "mycompany.queues.accounting", exchange: "mycompany.fanout.exchange", routingKey: "");
                        channel.QueueBind(queue: "mycompany.queues.management", exchange: "mycompany.fanout.exchange", routingKey: "");

                        IBasicProperties properties = channel.CreateBasicProperties();
                        properties.Persistent = true;
                        properties.ContentType = "text/plain";
                        PublicationAddress address = new PublicationAddress(exchangeType: ExchangeType.Fanout, exchangeName: "mycompany.fanout.exchange", routingKey: "");
                        channel.BasicPublish(addr: address, basicProperties: properties, body: Encoding.UTF8.GetBytes("A new huge order has just come in worth $1M!!!!!"));
                    }
                }
            }
            finally
            {
                readerWriterLock.ExitReadLock();
            }
        }

        private static IModel ReceiveFanoutMessages(IConnection connection, string queue)
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
                        Debug.WriteLine(string.Concat("Content type: ", basicProperties.ContentType));
                        Debug.WriteLine(string.Concat("Consumer tag: ", basicDeliveryEventArgs.ConsumerTag));
                        Debug.WriteLine(string.Concat("Delivery tag: ", basicDeliveryEventArgs.DeliveryTag));
                        string message = Encoding.UTF8.GetString(basicDeliveryEventArgs.Body);
                        Debug.WriteLine(string.Concat("Message: ", Encoding.UTF8.GetString(basicDeliveryEventArgs.Body)));
                        channel.BasicAck(basicDeliveryEventArgs.DeliveryTag, false);
                    }
                }
                finally
                {
                    readerWriterLock.ExitReadLock();
                }
            };

            channel.BasicConsume(queue: queue, autoAck: false, consumer: eventingBasicConsumer);

            return channel;
        }


        private static void SendSingleOneWayMessage(IConnection connection)
        {
            readerWriterLock.EnterReadLock();
            try
            {
                if(connection.IsOpen)
                {
                    using (IModel channel = connection.CreateModel())
                    {
                        channel.ExchangeDeclare("my.first.exchange", ExchangeType.Direct, true, false, null);
                        channel.QueueDeclare("my.first.queue", true, false, false, null);
                        channel.QueueBind("my.first.queue", "my.first.exchange", "");

                        IBasicProperties properties = channel.CreateBasicProperties();
                        properties.Persistent = true;
                        properties.ContentType = "text/plain";
                        PublicationAddress address = new PublicationAddress(ExchangeType.Direct, "my.first.exchange", "");
                        channel.BasicPublish(addr: address, basicProperties: properties, body: Encoding.UTF8.GetBytes("This is a message from the RabbitMq .NET driver"));
                    }
                }
            }
            finally
            {
                readerWriterLock.ExitReadLock();
            }
        }

        private static IModel ReceiveSingleOneWayMessage(IConnection connection)
        {
            IModel channel = connection.CreateModel();
            channel.BasicQos(0, 1, false);
            DefaultBasicConsumer basicConsumer = new OneWayMessageReceiver(channel);
            channel.BasicConsume(queue: "my.first.queue", autoAck: false, consumer: basicConsumer);
            return channel;
        }

        private static IModel ReceiveMessagesWithEvents(IConnection connection)
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
                        Debug.WriteLine(string.Concat("Content type: ", basicProperties.ContentType));
                        Debug.WriteLine(string.Concat("Consumer tag: ", basicDeliveryEventArgs.ConsumerTag));
                        Debug.WriteLine(string.Concat("Delivery tag: ", basicDeliveryEventArgs.DeliveryTag));
                        Debug.WriteLine(string.Concat("Message: ", Encoding.UTF8.GetString(basicDeliveryEventArgs.Body)));
                        channel.BasicAck(basicDeliveryEventArgs.DeliveryTag, false);
                    }
                }
                finally
                {
                    readerWriterLock.ExitReadLock();
                }
            };

            channel.BasicConsume(queue: "my.first.queue", autoAck: false, consumer: eventingBasicConsumer);
            return channel;
        }
    }
}
