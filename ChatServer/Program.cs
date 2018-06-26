using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.MessagePatterns;
using RabbitMQ.Client.Events;
using System.Threading;

namespace ChatServer
{
    class Program
    {
        static void Main(string[] args)
        {
            RunRpcQueue();
        }

        private static void RunRpcQueue()
        {
            ConnectionFactory connectionFactory = new ConnectionFactory
            {
                Port = 5672,
                HostName = "localhost",
                UserName = "accountant",
                Password = "accountant",
                VirtualHost = "accounting"
            };

            IConnection connection = connectionFactory.CreateConnection();
            IModel channel = connection.CreateModel();

            SendRpcMessagesBackAndForth(channel);
        }

        private static void SendRpcMessagesBackAndForth(IModel channel)
        {
            channel.QueueDeclare(queue: "mycompany.queues.rpc", durable: true, exclusive: false, autoDelete: false, arguments: null);

            string rpcResponseQueue = channel.QueueDeclare().QueueName;

            string correlationId = Guid.NewGuid().ToString();
            string responseFromConsumer = null;

            IBasicProperties basicProperties = channel.CreateBasicProperties();
            basicProperties.ReplyTo = rpcResponseQueue;
            basicProperties.CorrelationId = correlationId;

            EventingBasicConsumer rpcEventingBasicConsumer = new EventingBasicConsumer(channel);
            rpcEventingBasicConsumer.Received += (sender, basicDeliveryEventArgs) =>
            {
                IBasicProperties props = basicDeliveryEventArgs.BasicProperties;
                if (props != null
                    && props.CorrelationId == correlationId)
                {
                    string response = Encoding.UTF8.GetString(basicDeliveryEventArgs.Body);
                    responseFromConsumer = response;
                }
                channel.BasicAck(basicDeliveryEventArgs.DeliveryTag, false);
                Console.WriteLine("Response: {0}", responseFromConsumer);
                Console.WriteLine("Enter your message and press Enter.");
                var replyMessage = Console.ReadLine();
                var replayMessageBytes = Encoding.UTF8.GetBytes(replyMessage);
                channel.BasicPublish("", "mycompany.queues.rpc", basicProperties, replayMessageBytes);
            };
            channel.BasicConsume(queue: rpcResponseQueue, autoAck: false, consumer: rpcEventingBasicConsumer);

            Console.WriteLine("Enter your message and press Enter.");

            string message = Console.ReadLine();
            byte[] messageBytes = Encoding.UTF8.GetBytes(message);

            channel.BasicPublish(exchange: "", routingKey: "mycompany.queues.rpc", basicProperties: basicProperties, body: messageBytes);
        }
    }
}
