using System;
using RabbitMQ.Client;
using System.Text;
using Domain;

namespace EmitLog
{
    class Program
    {
        public static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "logs", type: "fanout");

                for (int j = 0; j < int.MaxValue; j++)
                {
                    var user = new User { Email = $"test{j}@test.ro", UserName = $"test{j}" };
                    for (int i = 0; i < 10; i++)
                    {
                        user.Addresses.Add(new Address { Number = i, Street = $"Victory Street {i}" });
                    }
                    channel.BasicPublish(exchange: "logs",
                     routingKey: "",
                     basicProperties: null,
                     body: user.ToRawBody(out string message));
                    Console.WriteLine(" [x] Sent {0}", message);
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
