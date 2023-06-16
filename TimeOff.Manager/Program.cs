using System.Net;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using TimeOff.Common;
using TimeOff.Manager.Messages;
using TimeOff.Models;

IConfiguration _configuration;
ConsumerConfig _consumerConfig;
ProducerConfig _producerConfig;
SchemaRegistryConfig _schemaRegistryConfig;
Queue<KafkaMessage> _leaveApplicationReceivedMessages;


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
_producerConfig = new()
{
    BootstrapServers = _configuration.GetValue<string>("ProducerConfig:BootstrapServers"),
    EnableDeliveryReports = _configuration.GetValue<bool>("ProducerConfig:EnableDeliveryReports"),
    ClientId = Dns.GetHostName()
};
_schemaRegistryConfig = new()
{
    Url = _configuration.GetValue<string>("SchemaRegistryConfig:Url")
};
_leaveApplicationReceivedMessages = new();


// Run the tasks and await until one is completed.
await Task.WhenAny(Task.Run(StartManagerConsumer, CancellationToken.None), Task.Run(StartLeaveApplicationProcessor, CancellationToken.None));


// Define all the tasks.
Task StartManagerConsumer()
{
    using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
    using var consumer = new ConsumerBuilder<string, LeaveApplicationReceived>(_consumerConfig)
        .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
        .SetValueDeserializer(new AvroDeserializer<LeaveApplicationReceived>(schemaRegistry).AsSyncOverAsync())
        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
        .Build();

    CancellationTokenSource cts = new();

    try
    {
        consumer.Subscribe(Constants.LeaveApplicationsTopicName);

        Console.WriteLine("Manager consumer started.");
        Console.WriteLine("Waiting for leave applications...");
        Console.WriteLine("");

        while (true)
        {
            try
            {
                var result = consumer.Consume(cts.Token);
                var leaveRequest = result?.Message?.Value;
                if (result == null || leaveRequest == null)
                    continue;

                // Adding message to a list just for the demo.
                // You should persist the message in the database and process it later.
                _leaveApplicationReceivedMessages.Enqueue(new KafkaMessage(result.Message.Key, result.Partition.Value, result.Message.Value));

                consumer.Commit(result);
                consumer.StoreOffset(result);
            }
            catch (ConsumeException e)
            {
                if (!e.Error.IsFatal)
                    Console.WriteLine($"Non fatal error: {e}");
            }
        }
    }
    catch (Exception)
    {
        cts.Cancel();
    }
    finally
    {
        consumer.Close();
    }

    return Task.CompletedTask;
}

async Task StartLeaveApplicationProcessor()
{
    while (true)
    {
        if (!_leaveApplicationReceivedMessages.Any())
        {
            Thread.Sleep(TimeSpan.FromSeconds(1));
            continue;
        }

        var (key, partition, leaveApplication) = _leaveApplicationReceivedMessages.Dequeue();

        Console.WriteLine($"Received message: {key} from partition {partition} with value {JsonSerializer.Serialize(leaveApplication)}");

        Console.WriteLine("Approve request? (Y/N):");
        bool isApproved = (Console.ReadLine() ?? "N").Equals("Y", StringComparison.OrdinalIgnoreCase);

        await SendMessageToResultTopicAsync(leaveApplication, isApproved, partition);
    }
}

async Task SendMessageToResultTopicAsync(LeaveApplicationReceived leaveRequest, bool isApproved, int partitionId)
{
    using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
    using var producer = new ProducerBuilder<string, LeaveApplicationProcessed>(_producerConfig)
        .SetKeySerializer(new AvroSerializer<string>(schemaRegistry).AsSyncOverAsync())
        .SetValueSerializer(new AvroSerializer<LeaveApplicationProcessed>(schemaRegistry).AsSyncOverAsync())
        .Build();

    LeaveApplicationProcessed leaveApplicationResult = new()
    {
        Email = leaveRequest.Email,
        Department = leaveRequest.Department,
        LeaveDurationInHours = leaveRequest.LeaveDurationInHours,
        LeaveStartDateTicks = leaveRequest.LeaveStartDateTicks,
        ProcessedBy = $"Manager {partitionId}",
        Result = isApproved
            ? "Approved: Your leave application has been approved."
            : "Declined: Your leave application has been declined."
    };

    var result = await producer.ProduceAsync(Constants.LeaveApplicationsResultsTopicName,
        new Message<string, LeaveApplicationProcessed>
        {
            Key = $"{leaveRequest.Email}-{DateTime.UtcNow.Ticks}",
            Value = leaveApplicationResult
        });

    Console.WriteLine($"Leave request processed and queued at offset {result.Offset.Value} in the topic {result.Topic}.");
    Console.WriteLine("");
}