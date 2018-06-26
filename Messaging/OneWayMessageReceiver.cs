using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Messaging
{
    public class OneWayMessageReceiver : DefaultBasicConsumer
    {
        private readonly IModel _channel;

        public OneWayMessageReceiver(IModel channel)
        {
            _channel = channel;
        }

        public override void HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IBasicProperties properties, byte[] body)
        {
            Debug.WriteLine(string.Concat("Message received from the exchange ", exchange));
            Debug.WriteLine(string.Concat("Content type: ", properties.ContentType));
            Debug.WriteLine(string.Concat("Consumer tag: ", consumerTag));
            Debug.WriteLine(string.Concat("Delivery tag: ", deliveryTag));
            Debug.WriteLine(string.Concat("Message: ", Encoding.UTF8.GetString(body)));
            _channel.BasicAck(deliveryTag, false);
        }
    }
}
