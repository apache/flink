package org.apache.flink.streaming.connectors.eventhubs.internals;

/**
 * Created by jozh on 5/22/2017.
 * Flink eventhub connnector has implemented with same design of flink kafka connector
 */
public enum EventhubOffsetType  {
	None,
	PreviousCheckpoint,
	InputByteOffset,
	InputTimeOffset
}
