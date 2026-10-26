using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations;
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

const string queueName = "hello";
const string replyQueueName = "hello_replies";

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } }
);

await channel.QueueDeclareAsync(
    queue: replyQueueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } }
);

var respostasPendentes = new ConcurrentDictionary<string, TaskCompletionSource<string>>();

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += (_, ea) =>
{
    var correlationId = ea.BasicProperties.CorrelationId;

    if (correlationId != null && respostasPendentes.TryRemove(correlationId, out var tcs))
    {
        var respostas = Encoding.UTF8.GetString(ea.Body.ToArray());
        tcs.TrySetResult(respostas);
    }

    return Task.CompletedTask;
};

await channel.BasicConsumeAsync(queue: replyQueueName, autoAck: false, consumer: consumer);

Console.WriteLine("Digite uma mensagem ou 'sair' para encerrar.");

while (true)
{
    Console.Write("Mensagem: ");
    var mensagem = Console.ReadLine();
    if (string.IsNullOrWhiteSpace(mensagem))
        continue;

    if (mensagem.Equals("sair", StringComparison.OrdinalIgnoreCase))
        break;

    var correlationId = Guid.NewGuid().ToString();

    var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

    respostasPendentes[correlationId] = tcs;

    var propriedades = new BasicProperties
    {
        ReplyTo = replyQueueName,
        CorrelationId = correlationId,
    };

    var body = Encoding.UTF8.GetBytes(mensagem);

    await channel.BasicPublishAsync(
        exchange: string.Empty,
        routingKey: queueName,
        mandatory: false,
        basicProperties: propriedades,
        body: body
    );

    var timout = Task.Delay(TimeSpan.FromSeconds(10));
    var resultado = await Task.WhenAny(tcs.Task, timout);

    if (resultado == tcs.Task)
    {
        Console.WriteLine($"[✓] Resposta: {await tcs.Task}");
    }
    else
    {
        respostasPendentes.TryRemove(correlationId, out _);
        Console.WriteLine("[!] Tempo esgotado. Nenhuma resposta recebida.");
    }
}

Console.WriteLine("Produtor encerrado");
