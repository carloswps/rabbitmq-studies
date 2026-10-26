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
