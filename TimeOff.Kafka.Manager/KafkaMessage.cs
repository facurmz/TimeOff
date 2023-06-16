using TimeOff.Kafka.Common.Models;

namespace TimeOff.Kafka.Manager;

public record KafkaMessage
(
	string Key,
	int Partition,
	LeaveApplicationReceived Message
);
