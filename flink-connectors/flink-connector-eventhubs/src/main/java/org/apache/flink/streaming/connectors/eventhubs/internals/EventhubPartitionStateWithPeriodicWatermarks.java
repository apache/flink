package org.apache.flink.streaming.connectors.eventhubs.internals;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Created by jozh on 6/16/2017.
 * Flink eventhub connnector has implemented with same design of flink kafka connector
 */

public class EventhubPartitionStateWithPeriodicWatermarks<T> extends EventhubPartitionState {
	private final AssignerWithPeriodicWatermarks<T> timestampsAndWatermarks;
	private long partitionWatermark;

	public EventhubPartitionStateWithPeriodicWatermarks(EventhubPartition key, String value, AssignerWithPeriodicWatermarks<T> timestampsAndWatermarks) {
		super(key, value);
		this.timestampsAndWatermarks = timestampsAndWatermarks;
		this.partitionWatermark = Long.MIN_VALUE;
	}

	public long getTimestampForRecord(T record, long kafkaEventTimestamp) {
		return timestampsAndWatermarks.extractTimestamp(record, kafkaEventTimestamp);
	}

	public long getCurrentWatermarkTimestamp() {
		Watermark wm = timestampsAndWatermarks.getCurrentWatermark();
		if (wm != null) {
			partitionWatermark = Math.max(partitionWatermark, wm.getTimestamp());
		}
		return partitionWatermark;
	}

	@Override
	public String toString() {
		return "EventhubPartitionStateWithPeriodicWatermarks: partition=" + getPartition()
			+ ", offset=" + getOffset() + ", watermark=" + partitionWatermark;
	}
}
