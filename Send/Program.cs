using Domain;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Send
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "hello",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                for (int j = 0; j < int.MaxValue; j++)
                {
                    var user = new User { Email = $"test{j}@test.ro", UserName = $"test{j}" };
                    for (int i = 0; i < 10; i++)
                    {
                        user.Addresses.Add(new Address { Number = i, Street = $"Victory Street {i}" });
                    }
                    Send(channel, user, "hello");
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        private static void Send<T>(IModel channel, T input, string queue)
        {
            var rawBody = input.ToRawBody(out string message);
            channel.BasicPublish(exchange: "",
                                 routingKey: queue,
                                 basicProperties: null,
                                 body: rawBody);
            Console.WriteLine(" [x] Sent {0}", message);
        }
    }
}
