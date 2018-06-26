using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Worker
{
    class Program
    {
        static EventHandler<BasicDeliverEventArgs> CreateWork(string workerId, IModel channel, int seconds)
        {
            return (model, ea) =>
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [{1}] Received {0}", message, workerId);
                Thread.Sleep(seconds * 1000);
                Console.WriteLine(" [{0}] Done", workerId);

                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            };
        }

        static AsyncEventHandler<BasicDeliverEventArgs> CreateWorkAsync(string workerId, IModel channel, int seconds)
        {
            return async (model, ea) =>
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [{1}] Received {0}", message, workerId);
                await Task.Delay(seconds * 1000);
                Console.WriteLine(" [{0}] Done", workerId);

                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            };
        }

        public static void Main()
        {
            var factory = new ConnectionFactory() { HostName = "localhost", DispatchConsumersAsync = false };
            using (var connection1 = factory.CreateConnection())
            using (var channel1 = connection1.CreateModel())
            using (var channel2 = connection1.CreateModel())
            using (var channel3 = connection1.CreateModel())
            {
                CreateChannel(channel1, "connection 1 ", 0);
                CreateChannel(channel2, "connection 2 ", 0);
                CreateChannel(channel3, "connection 3 ", 0);
                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        private static void CreateChannel(IModel channel, string connectionId, int seconds)
        {
            channel.QueueDeclare(queue: "task_queue",
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            Console.WriteLine(" [*] Waiting for messages.");

            var consumer1 = new EventingBasicConsumer(channel);
            consumer1.Received += CreateWork(connectionId + "test 1", channel, seconds);
            channel.BasicConsume(queue: "task_queue",
                                autoAck: false,
                                consumer: consumer1);

            //var consumer3 = new EventingBasicConsumer(channel);
            //consumer3.Received += CreateWork("test 2", channel, 0);
            //channel.BasicConsume(queue: "task_queue",
            //                    autoAck: false,
            //                    consumer: consumer3);

            //var consumer2 = new AsyncEventingBasicConsumer(channel);
            //consumer2.Received += CreateWorkAsync(connectionId + "bac 2", channel, seconds);
            //channel.BasicConsume(queue: "task_queue",
            //                  autoAck: false,
            //                  consumer: consumer2);

            //var consumer3 = new AsyncEventingBasicConsumer(channel);
            //consumer3.Received += CreateWorkAsync(connectionId + "bac 3", channel, seconds);
            //channel.BasicConsume(queue: "task_queue",
            //                  autoAck: false,
            //                  consumer: consumer3);
        }
    }
}
