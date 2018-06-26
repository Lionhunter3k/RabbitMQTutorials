using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading;

namespace ReceiveLogsDirect
{
    class Program
    {
        public static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel1 = connection.CreateModel())
            using (var channel2 = connection.CreateModel())
            using (var channel3 = connection.CreateModel())
            {
                SetupModel(channel1, 0, "info");
                SetupModel(channel2, 2, "debug", "warning");
                SetupModel(channel3, 10, "error");
                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        private static void SetupModel(IModel channel, int delayInSeconds, params string[] logLevels)
        {
            channel.ExchangeDeclare(exchange: "direct_logs",
                                    type: "direct");

            var queueName = channel.QueueDeclare().QueueName;

            foreach (var severity in logLevels)
            {
                channel.QueueBind(queue: queueName,
                                  exchange: "direct_logs",
                                  routingKey: severity);
            }

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body);
                var routingKey = ea.RoutingKey;
                Thread.Sleep(delayInSeconds * 1000);
                Console.WriteLine(" [x] Received '{0}':'{1}'",
                                  routingKey, message);
            };
            channel.BasicConsume(queue: queueName,
                                 autoAck: true,
                                 consumer: consumer);
        }
    }
}
