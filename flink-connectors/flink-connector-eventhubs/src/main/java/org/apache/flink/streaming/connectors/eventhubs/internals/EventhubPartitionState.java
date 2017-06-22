package org.apache.flink.streaming.connectors.eventhubs.internals;

/**
 * Created by jozh on 5/23/2017.
 * Flink eventhub connnector has implemented with same design of flink kafka connector
 */

public class EventhubPartitionState {
	private final EventhubPartition partition;
	private volatile String offset;

	public EventhubPartitionState(EventhubPartition partition, String offset){
		this.partition = partition;
		this.offset = offset;
	}

	public final String getOffset() {
		return  this.offset;
	}

	public final void  setOffset(String offset) {
		this.offset = offset;
	}

	public EventhubPartition getPartition() {
		return this.partition;
	}
}

