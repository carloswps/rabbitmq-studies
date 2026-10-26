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

/* const string queueName = "hello"; */

await channel.QueueDeclareAsync(
    queue: "task_queue",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } }
);

var properties = new BasicProperties { Persistent = true };
