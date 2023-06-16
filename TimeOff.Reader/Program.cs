using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using TimeOff.Common;
using TimeOff.Models;

IConfiguration _configuration;
ConsumerConfig _consumerConfig;
SchemaRegistryConfig _schemaRegistryConfig;

// Build the configuration from the appsettings.json file.
_configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", true, true)
    .Build();


// Build the configuration for every item.
_consumerConfig = new()
{
    BootstrapServers = _configuration.GetValue<string>("ConsumerConfig:BootstrapServers"),
    GroupId = _configuration.GetValue<string>("ConsumerConfig:GroupId"),
    EnableAutoCommit = _configuration.GetValue<bool>("ConsumerConfig:EnableAutoCommit"),
    EnableAutoOffsetStore = _configuration.GetValue<bool>("ConsumerConfig:EnableAutoOffsetStore"),
    AutoOffsetReset = AutoOffsetReset.Earliest
};
_schemaRegistryConfig = new()
{
    Url = _configuration.GetValue<string>("SchemaRegistryConfig:Url")
};


// // Build the SchemaRegistry and Producer.
using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
using var consumer = new ConsumerBuilder<string, LeaveApplicationProcessed>(_consumerConfig)
    .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
    .SetValueDeserializer(new AvroDeserializer<LeaveApplicationProcessed>(schemaRegistry).AsSyncOverAsync())
    .Build();

CancellationTokenSource cts = new();

try
{
    consumer.Subscribe(Constants.LeaveApplicationsResultsTopicName);

    Console.WriteLine("Reader consumer started.");
    Console.WriteLine("Waiting for leave applications results...");
    Console.WriteLine("");

    while (true)
    {
        var result = consumer.Consume(cts.Token);

        LeaveApplicationProcessed leaveRequest = result.Message.Value;

        Console.WriteLine($"Received message: {result.Message.Key} with value {JsonSerializer.Serialize(leaveRequest)}");

        consumer.Commit(result);
        consumer.StoreOffset(result);

        Console.WriteLine("Offset committed.");
        Console.WriteLine("");
    }
}
catch
{
    cts.Cancel();
}
finally
{
    consumer.Close();
}