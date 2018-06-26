using System;
using System.Linq;
using RabbitMQ.Client;
using System.Text;
using System.Collections.Generic;
using Domain;

namespace EmitLogTopic
{
    class Program
    {
        public static void Main(string[] args)
        {
            var logLevels = new List<string> { "debug", "info", "warning", "error" };
            var source = new List<string> { "anonymous"};
            for(var i = 0; i < 10000; i++)
            {
                source.Add("client_" + i);
            }
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "topic_logs",
                                        type: "topic");
                for (int j = 0; j < int.MaxValue; j++)
                {
                    var routingKey = source[j % source.Count] + "." + logLevels[j % logLevels.Count];
                    var user = new User { Email = $"test{j}@test.ro", UserName = $"test{j}" };
                    for (int i = 0; i < 10; i++)
                    {
                        user.Addresses.Add(new Address { Number = i, Street = $"Victory Street {i}" });
                    }
                    channel.BasicPublish(exchange: "topic_logs",
                     routingKey: routingKey,
                     basicProperties: null,
                     body: user.ToRawBody(out string message));
                    Console.WriteLine(" [x] Sent '{0}':'{1}'", routingKey, message);
                }
            }
        }
    }
}
