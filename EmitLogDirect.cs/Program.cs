using System;
using System.Linq;
using RabbitMQ.Client;
using System.Text;
using Domain;
using System.Collections.Generic;

namespace EmitLogDirect.cs
{
    class Program
    {
        public static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "direct_logs",
                                        type: "direct");

                var logLevels = new List<string> { "debug", "info", "warning", "error" };
                for (int j = 0; j < int.MaxValue; j++)
                {
                    var requiredLogLevel = j % logLevels.Count;
                    var user = new User { Email = $"test{j}@test.ro", UserName = $"test{j}" };
                    for (int i = 0; i < 10; i++)
                    {
                        user.Addresses.Add(new Address { Number = i, Street = $"Victory Street {i}" });
                    }
                    channel.BasicPublish(exchange: "direct_logs",
                     routingKey: logLevels[requiredLogLevel],
                     basicProperties: null,
                     body: user.ToRawBody(out string message));
                    Console.WriteLine(" [x] Sent '{0}':'{1}'", logLevels[requiredLogLevel], message);
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
