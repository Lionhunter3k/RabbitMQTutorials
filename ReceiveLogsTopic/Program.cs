using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace ReceiveLogsTopic
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
                //SetupChannel(channel1, 2, "#");
                //SetupChannel(channel2, 0, "*.warning", "*.debug");
                SetupChannel(channel3, 10, "client_2.#");

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        private static void SetupChannel(IModel channel, int delayInSeconds, params string[] sourceLogLevels)
        {
            channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");
            var queueName = channel.QueueDeclare().QueueName;

            foreach (var bindingKey in sourceLogLevels)
            {
                channel.QueueBind(queue: queueName,
                                  exchange: "topic_logs",
                                  routingKey: bindingKey);
            }

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body);
                var routingKey = ea.RoutingKey;
                Console.WriteLine(" [x] Received '{0}':'{1}'",
                                  routingKey,
                                  message);
            };
            channel.BasicConsume(queue: queueName,
                                 autoAck: true,
                                 consumer: consumer);
        }
    }
}
