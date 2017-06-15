package org.apache.flink.streaming.connectors.eventhubs;

/**
 * Created by jozh on 5/22/2017.
 */
public enum EventhubOffsetType  {
	None,
	PreviousCheckpoint,
	InputByteOffset,
	InputTimeOffset
}
