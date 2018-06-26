using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RPCClient
{
    public class AsyncRpcClient : IDisposable
    {
        private readonly IConnection connection;
        private readonly IModel channel;
        private readonly string replyQueueName;
        private readonly EventingBasicConsumer consumer;
        private readonly IBasicProperties props;
        private readonly Dictionary<string, TaskCompletionSource<int>> _pendingTasks = new Dictionary<string, TaskCompletionSource<int>>();

        public void Dispose()
        {
            channel.Dispose();
            connection.Dispose();
        }

        public AsyncRpcClient()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            connection = factory.CreateConnection();
            channel = connection.CreateModel();

            replyQueueName = channel.QueueDeclare().QueueName;
            consumer = new EventingBasicConsumer(channel);

            props = channel.CreateBasicProperties();
            props.ReplyTo = replyQueueName;

            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                if (_pendingTasks.Remove(ea.BasicProperties.CorrelationId, out TaskCompletionSource<int> deffered))
                    if (int.TryParse(Encoding.UTF8.GetString(body), out int response))
                    {
                        deffered.SetResult(response);
                    }
                    else
                        {
                            deffered.SetException(new FormatException());
                        }
            };
        }

        public Task<int> CallAsync(int number)
        {
            var messageBytes = Encoding.UTF8.GetBytes(number.ToString());

            props.CorrelationId = Guid.NewGuid().ToString();

            channel.BasicPublish(
                exchange: "",
                routingKey: "rpc_queue",
                basicProperties: props,
                body: messageBytes);

            channel.BasicConsume(
                consumer: consumer,
                queue: replyQueueName,
                autoAck: true);

            var deffered = new TaskCompletionSource<int>();
            _pendingTasks.Add(props.CorrelationId, deffered);
            return deffered.Task;
        }
    }

    public class RpcClient
    {
        private readonly IConnection connection;
        private readonly IModel channel;
        private readonly string replyQueueName;
        private readonly EventingBasicConsumer consumer;
        private readonly BlockingCollection<string> respQueue = new BlockingCollection<string>();
        private readonly IBasicProperties props;

        public RpcClient()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            connection = factory.CreateConnection();
            channel = connection.CreateModel();
            replyQueueName = channel.QueueDeclare().QueueName;
            consumer = new EventingBasicConsumer(channel);

            props = channel.CreateBasicProperties();
            var correlationId = Guid.NewGuid().ToString();
            props.CorrelationId = correlationId;
            props.ReplyTo = replyQueueName;

            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var response = Encoding.UTF8.GetString(body);
                if (ea.BasicProperties.CorrelationId == correlationId)
                {
                    respQueue.Add(response);
                }
            };
        }

        public string Call(string message)
        {
            var messageBytes = Encoding.UTF8.GetBytes(message);
            channel.BasicPublish(
                exchange: "",
                routingKey: "rpc_queue",
                basicProperties: props,
                body: messageBytes);

            channel.BasicConsume(
                consumer: consumer,
                queue: replyQueueName,
                autoAck: true);

            return respQueue.Take(); ;
        }

        public void Close()
        {
            connection.Close();
        }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            var rpcClient = new AsyncRpcClient();

            Console.WriteLine(" [x] Requesting fib(3)");
            var response = await rpcClient.CallAsync(12);

            Console.WriteLine(" [.] Got '{0}'", response);
            rpcClient.Dispose();
        }
    }
}
