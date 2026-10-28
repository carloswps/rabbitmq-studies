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

channel.BasicReturnAsync += (sender, ea) =>
{
    var returnedMessage = Encoding.UTF8.GetString(ea.Body.ToArray());

    Console.WriteLine("\n[!] ERROR: message not routed!");
    Console.WriteLine($"    Reason: {ea.ReplyText}");
    Console.WriteLine($"    Key: {ea.RoutingKey}");
    Console.WriteLine($"    Message: {returnedMessage}");

    return Task.CompletedTask;
};

const string queueName = "task_queue";

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } }
);

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
    /*
     await channel.BasicPublishAsync(
         exchange: string.Empty,
         routingKey: "incorrect_routing_key",
         mandatory: true,
         basicProperties: properties,
         body: body
     ); */

    await channel.BasicPublishAsync(
        exchange: "app.direct",
        routingKey: "order.created",
        mandatory: true,
        basicProperties: properties,
        body: body
    );

    Console.WriteLine("Publicação enviada ao RabbitMQ.");
}
