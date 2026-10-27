using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory
{
    HostName = "localhot",
    Port = 5672,
    UserName = "app",
    Password = "senha_dev_123",
};

using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

const string exchangeName = "app.direct";
const string queueName = "orders_queue";
const string routingKey = "order.created";

await channel.ExchangeDeclareAsync(
    exchange: exchangeName,
    type: ExchangeType.Direct,
    durable: true
);

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } }
);

await channel.QueueBindAsync(queue: queueName, exchange: exchangeName, routingKey: routingKey);

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (model, ea) =>
{
    var message = Encoding.UTF8.GetString(ea.Body.ToArray());

    Console.WriteLine($"Message received {message}");
    Console.WriteLine($"Routing key: {ea.RoutingKey}");

    await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
};

await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);

Console.WriteLine("Consumer waiting for messages. Press Ctrl+C to exit.");

await Task.Delay(Timeout.Infinite);
