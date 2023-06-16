using System.Globalization;
using System.Net;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using TimeOff.Kafka.Common;
using TimeOff.Kafka.Common.Models;
using TimeOff.Kafka.Employee;

IConfiguration _configuration;
AdminClientConfig _adminConfig;
ProducerConfig _producerConfig;
SchemaRegistryConfig _schemaRegistryConfig;

// Build the configuration from the appsettings.json file.
_configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", true, true)
    .Build();


// Build the configuration for every item.
_adminConfig = new() {
    BootstrapServers = _configuration.GetValue<string>("AdminClientConfig:BootstrapServers")
};
_producerConfig = new() {
    BootstrapServers = _configuration.GetValue<string>("ProducerConfig:BootstrapServers"),
    EnableDeliveryReports = _configuration.GetValue<bool>("ProducerConfig:EnableDeliveryReports"),
    ClientId = Dns.GetHostName()
};
_schemaRegistryConfig = new() {
    Url = _configuration.GetValue<string>("SchemaRegistryConfig:Url")
};


// Specify the name of the topic.
List<TopicSpecification> topics = new();

TopicSpecification topic = new() {
    Name = Constants.LeaveApplicationsTopicName,
    ReplicationFactor = 1,
    NumPartitions = 3
};

topics.Add(topic);


// Build the AdminClient to create the topic.
using var adminClient = new AdminClientBuilder(_adminConfig).Build();

try
{
    await adminClient.CreateTopicsAsync(topics);
}
catch (CreateTopicsException e)
{
    if (e.Results.Select(r => r.Error.Code).Any(ec => ec == ErrorCode.TopicAlreadyExists))
        Console.WriteLine($"Topic {e.Results[0].Topic} already exists.");
}


// Build the SchemaRegistry and Producer.
using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
using var producer = new ProducerBuilder<string, LeaveApplicationReceived>(_producerConfig)
    .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
    .SetValueSerializer(new AvroSerializer<LeaveApplicationReceived>(schemaRegistry))
    .Build();

Console.WriteLine("Employee producer started.");
Console.WriteLine("Preparing to submit application requests...");
Console.WriteLine("");

while (true)
{
    Console.WriteLine("Enter your employee email (e.g. example@your-company.com):");
    string? cEmail = Console.ReadLine();
    string email = (cEmail == null || cEmail == "") ? "example@your-company.com" : cEmail;

    Console.WriteLine("Enter your department code (HR, IT or OPS):");
    string? cDepartment = Console.ReadLine();
    string department = (cDepartment == null || cDepartment == "")  ? "IT" : cDepartment;

    Console.WriteLine("Enter the number of hours of leave requested (e.g. 8):");
    string? cLeaveDurationInHours = Console.ReadLine();
    int leaveDurationInHours = (cLeaveDurationInHours == null || cLeaveDurationInHours == "") ? 8 : int.Parse(cLeaveDurationInHours);

    Console.WriteLine("Enter your leave start date (DD-MM-YYYY):");
    string? cLeaveStartDate = Console.ReadLine();
    DateTime leaveStartDate = (cLeaveStartDate == null || cLeaveStartDate == "") ? DateTime.Now.AddDays(30) : DateTime.ParseExact(cLeaveStartDate, "dd-mm-yyyy", CultureInfo.InvariantCulture);

    LeaveApplicationReceived leaveApplication = new()
    {
        Email = email,
        Department = department,
        LeaveDurationInHours = leaveDurationInHours,
        LeaveStartDateTicks = leaveStartDate.Ticks
    };

    TopicPartition partition = new(topic.Name, new Partition((int)Enum.Parse<Departments>(department)));

    DeliveryResult<string, LeaveApplicationReceived> result = await producer.ProduceAsync(partition, new Message<string, LeaveApplicationReceived> {
        Key = $"{email}-{DateTime.UtcNow.Ticks}",
        Value = leaveApplication
    });

    Console.WriteLine($"Your leave request is queued at offset {result.Offset.Value} in the topic {result.Topic}:{result.Partition.Value}.");
    Console.WriteLine("");
}
