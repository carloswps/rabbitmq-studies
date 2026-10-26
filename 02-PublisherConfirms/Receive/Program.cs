using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory
{
    HostName = "localhost",
    Port = 5672,
    UserName = "app",
    Password = "senha_dev_123",
};

using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

var queueName = "hello";

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } }
);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (_, ea) =>
{
    var mensagem = Encoding.UTF8.GetString(ea.Body.ToArray());

    Console.WriteLine($"[x] Recebido: {mensagem}");

    await Task.Delay(500);

    var resposta = Encoding.UTF8.GetBytes($"Processado com sucesso: {mensagem}");

    var proriedades = new BasicProperties { CorrelationId = ea.BasicProperties.CorrelationId };

    if (!string.IsNullOrEmpty(ea.BasicProperties.ReplyTo))
    {
        await channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: ea.BasicProperties.ReplyTo,
            mandatory: false,
            basicProperties: proriedades,
            body: resposta
        );
    }

    await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);

    Console.WriteLine("[x] ACK enviado ao RabbitMQ");
};

await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);

Console.WriteLine("[*] Aguardando mensagens. Pressione ENTER para sair.");
Console.ReadLine();
