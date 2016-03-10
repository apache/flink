/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class FlinkKafkaComsumerWithWMBase<T> extends FlinkKafkaConsumerBase<T> {


	/** Keeps track of the minimum timestamp seen, per Kafka topic per partition */
	private final Map<String, Map<Integer, Long>> minSeenTimestampsPerTopicAndPartition = new HashMap<String, Map<Integer, Long>>();

	/** Tracks the last emitted watermark. */
	protected Long lastEmittedWatermark = Long.MIN_VALUE;

	/**
	 * Creates a new Flink Kafka Consumer, using the given type of fetcher and offset handler.
	 * <p>
	 * <p>To determine which kink of fetcher and offset handler to use, please refer to the docs
	 * at the beginning of this class.</p>
	 *
	 * @param deserializer The deserializer to turn raw byte messages into Java/Scala objects.
	 * @param props
	 */
	public FlinkKafkaComsumerWithWMBase(KeyedDeserializationSchema<T> deserializer, Properties props) {
		super(deserializer, props);
	}

	/**
	 * Sets the minimum timestamp seen for a given partition of a given topic to {@code timestamp},
	 * if the provided {@code timestamp} is smaller than the already seen so far.
	 * @param topic
	 *           The topic we are interested in.
	 * @param partition
	 *           The partition we are interested in.
	 * @param timestamp
	 *           The timestamp to set the minimum to, if smaller than the already existing one.
	 * @return {@code true} if the minimum was updated successfully to {@code timestamp}, {@code false}
	 *           if the previous value is smaller than the provided timestamp
	 * */
	protected boolean updateMinimumTimestampForPartition(String topic, int partition, long timestamp) {
		Map<Integer, Long> minTimestampsForTopic =
			getMinTimestampsForPartitionsOfTopic(topic);

		Long minTimestampForPartition = minTimestampsForTopic.get(partition);
		if(minTimestampForPartition == null || timestamp < minTimestampForPartition) {
			minTimestampsForTopic.put(partition, timestamp);
			return true;
		}
		return false;
	}

	/**
	 * Returns the minimum timestamp seen across ALL topics and ALL local partitions.
	 * */
	protected long getMinimumTimestampAcrossAllTopics() {
		long globalMinTimestamp = Long.MAX_VALUE;
		for(String topic: minSeenTimestampsPerTopicAndPartition.keySet()) {
			long minForTopic = getMinimumTimestampForTopic(topic);
			if(minForTopic < globalMinTimestamp) {
				globalMinTimestamp = minForTopic;
			}
		}
		return globalMinTimestamp == Long.MAX_VALUE ? Long.MIN_VALUE : globalMinTimestamp;
	}

	/**
	 * Emits a new watermark, if it signals progress in event-time. This means
	 * that the new Watermark (with timestamp = {@code timestamp} will be emitted if
	 * and only if {@timestamp} is greater than the timestamp of the last emitted
	 * watermark.
	 * @return {@code true} if the Watermark was successfully emitted, {@code false} otherwise.
	 */
	protected boolean emitWatermarkIfMarkingProgress(SourceFunction.SourceContext<T> sourceContext, long wmTimestamp) {
		if(wmTimestamp > lastEmittedWatermark) {
			lastEmittedWatermark = wmTimestamp;
			Watermark toEmit = new Watermark(wmTimestamp);
			sourceContext.emitWatermark(toEmit);
			return true;
		}
		return false;
	}

	/**
	 * Returns the minimum timestamp seen across all local partition for a given topic.
	 * @param topic
	 *           The topic we are interested in.
	 * */
	private long getMinimumTimestampForTopic(String topic) {
		Map<Integer, Long> minTimestampsForTopic =
			getMinTimestampsForPartitionsOfTopic(topic);

		long minTimestamp = Long.MAX_VALUE;
		for(Long ts: minTimestampsForTopic.values()){
			if(ts < minTimestamp) {
				minTimestamp = ts;
			}
		}
		return minTimestamp == Long.MAX_VALUE ? Long.MIN_VALUE : minTimestamp;
	}

	/**
	 * Returns a map with the minimum timestamp seen per partition of the given topic.
	 * @param topic
	 *           The topic we are interested in.
	 * */
	private Map<Integer, Long> getMinTimestampsForPartitionsOfTopic(String topic) {
		Map<Integer, Long> minTimestampsForTopic = minSeenTimestampsPerTopicAndPartition.get(topic);
		if(minTimestampsForTopic == null) {
			minSeenTimestampsPerTopicAndPartition.put(topic, new HashMap<Integer, Long>());
		}
		return minTimestampsForTopic;
	}

}
