using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory
{
    HostName = "localhost",
    Port = 5672,
    UserName = "app",
    Password = "senha_dev_123",
};

using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

const string exchangeName = "app.direct";
const string routingKey = "order.created";

await channel.ExchangeDeclareAsync(
    exchange: exchangeName,
    type: ExchangeType.Direct,
    durable: true
);

string message = "Order #123 created";
byte[] body = Encoding.UTF8.GetBytes(message);

await channel.BasicPublishAsync(exchange: exchangeName, routingKey: routingKey, body: body);

Console.WriteLine($"Message send: {message}");
Console.WriteLine($"Exchange: {exchangeName}");
Console.WriteLine($"Routing key: {routingKey}");
