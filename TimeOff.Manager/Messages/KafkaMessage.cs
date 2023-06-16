using TimeOff.Models;

namespace TimeOff.Manager.Messages;

public record KafkaMessage
(
	string Key,
	int Partition,
	LeaveApplicationReceived Message
);
