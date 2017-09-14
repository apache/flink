/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.eventhubs.internals;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * Created by jozh on 6/16/2017.
 * Flink eventhub connnector has implemented with same design of flink kafka connector
 */

public class EventhubPartitionStateWithPunctuatedWatermarks<T> extends EventhubPartitionState {
	private final AssignerWithPunctuatedWatermarks<T> timestampsAndWatermarks;
	private long partitionWatermark;

	public EventhubPartitionStateWithPunctuatedWatermarks(EventhubPartition key, String value, AssignerWithPunctuatedWatermarks<T> timestampsAndWatermarks) {
		super(key, value);
		this.timestampsAndWatermarks = timestampsAndWatermarks;
		this.partitionWatermark = Long.MIN_VALUE;
	}

	public long getTimestampForRecord(T record, long kafkaEventTimestamp) {
		return timestampsAndWatermarks.extractTimestamp(record, kafkaEventTimestamp);
	}

	@Nullable
	public Watermark checkAndGetNewWatermark(T record, long timestamp) {
		Watermark mark = timestampsAndWatermarks.checkAndGetNextWatermark(record, timestamp);
		if (mark != null && mark.getTimestamp() > partitionWatermark) {
			partitionWatermark = mark.getTimestamp();
			return mark;
		}
		else {
			return null;
		}
	}

	public long getCurrentPartitionWatermark() {
		return partitionWatermark;
	}

	@Override
	public String toString() {
		return "EventhubPartitionStateWithPunctuatedWatermarks: partition=" + getPartition()
			+ ", offset=" + getOffset() + ", watermark=" + partitionWatermark;
	}
}
