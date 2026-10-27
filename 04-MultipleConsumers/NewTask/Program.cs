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

const string queueName = "task_queue";

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } }
);

Console.WriteLine("Digite uma mensagem ou 'sair' para encerrar.");

while (true)
{
    Console.Write("Mensagem: ");
    var message = Console.ReadLine();
    if (string.IsNullOrWhiteSpace(message))
        continue;

    if (message.Equals("sair", StringComparison.OrdinalIgnoreCase))
        break;

    var properties = new BasicProperties { Persistent = true };

    var body = Encoding.UTF8.GetBytes(message);

    await channel.BasicPublishAsync(
        exchange: string.Empty,
        routingKey: "task_queue",
        mandatory: true,
        basicProperties: properties,
        body: body
    );

    var correlationId = Guid.NewGuid().ToString();

    var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
}
