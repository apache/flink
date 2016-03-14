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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class FlinkKafkaComsumerWithWMBase<T> extends FlinkKafkaConsumerBase<T> {


	/** Keeps track of the minimum timestamp seen, per Kafka topic per partition */
	private final Map<String, Map<Integer, Long>> maxTsPerTopicAndPartition = new HashMap<String, Map<Integer, Long>>();

	private final Map<String, Map<Integer, Boolean>> isPartitionEligibleForWM = new HashMap<>();

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
	 * Emits a new watermark, if it signals progress in event-time. This means
	 * that the new Watermark (with timestamp = {@code timestamp} will be emitted if
	 * and only if {@timestamp} is greater than the timestamp of the last emitted
	 * watermark.
	 * @return {@code true} if the Watermark was successfully emitted, {@code false} otherwise.
	 */
	protected boolean emitWatermarkIfMarkingProgress(SourceFunction.SourceContext<T> sourceContext) {
		Tuple3<String, Integer, Long> globalMinTs = getMinTimestampAcrossAllTopics();
		String topic = globalMinTs.f0;
		if(topic != null ) {
			synchronized (sourceContext.getCheckpointLock()) {
				int partition = globalMinTs.f1;
				long minTs = globalMinTs.f2;

				if(minTs > lastEmittedWatermark) {
					lastEmittedWatermark = minTs;
					Watermark toEmit = new Watermark(minTs);
					setEligibleFlagForPartition(topic, partition, false);
					sourceContext.emitWatermark(toEmit);
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Returns the minimum timestamp seen across ALL topics and ALL local partitions.
	 * */
	private Tuple3<String, Integer, Long> getMinTimestampAcrossAllTopics() {
		Tuple3<String, Integer, Long> globalMinTs = new Tuple3<>(null, -1, Long.MAX_VALUE);
		for(String topic: maxTsPerTopicAndPartition.keySet()) {
			Tuple2<Integer, Long> minForTopic = getMinTimestampForTopic(topic);
			if(minForTopic.f1 < globalMinTs.f2) {
				globalMinTs.f0 = topic;
				globalMinTs.f1 = minForTopic.f0;
				globalMinTs.f2 = minForTopic.f1;
			}
		}

		if(globalMinTs.f2 == Long.MAX_VALUE) {
			globalMinTs.f2 = Long.MIN_VALUE;
		}
		return globalMinTs;
	}

	/**
	 *
	 * Returns the minimum timestamp for a topic among the max timestamps seen for each of its partitions.
	 * @param topic
	 *           The topic we are interested in.
	 * */
	private Tuple2<Integer, Long> getMinTimestampForTopic(String topic) {
		Map<Integer, Long> maxTsForTopic =
			getMaxTimestampsForPartitionsOfTopic(topic);

		Tuple2<Integer, Long> res = new Tuple2<>(-1, Long.MAX_VALUE);
		for(Integer part: maxTsForTopic.keySet()){
			Long ts = maxTsForTopic.get(part);
			if(isPartitionEligible(topic, part) && ts < res.f1) {
				res.f0 = part;
				res.f1 = ts;
			}
		}
		return res;
	}

	/**
	 * Sets the maximum timestamp seen for a given partition of a given topic to {@code timestamp},
	 * if the provided {@code timestamp} is greater than the one already seen so far.
	 * @param topic
	 *           The topic we are interested in.
	 * @param partition
	 *           The partition we are interested in.
	 * @param timestamp
	 *           The timestamp to set the minimum to, if smaller than the already existing one.
	 * @return {@code true} if the minimum was updated successfully to {@code timestamp}, {@code false}
	 *           if the previous value is smaller than the provided timestamp
	 * */
	protected boolean updateMaximumTimestampForPartition(String topic, int partition, long timestamp) {
		Map<Integer, Long> maxTsForTopic =
			getMaxTimestampsForPartitionsOfTopic(topic);

		Long maxTsForPartition = maxTsForTopic.get(partition);
		if(maxTsForPartition == null || timestamp > maxTsForPartition) {
			setEligibleFlagForPartition(topic, partition, true);
			maxTsForTopic.put(partition, timestamp);
			return true;
		}
		return false;
	}

	private void setEligibleFlagForPartition(String topic, int partition, boolean isEligible) {
		Map<Integer, Boolean> topicInfo = isPartitionEligibleForWM.get(topic);
		if(topicInfo == null) {
			throw new RuntimeException("Unknown Topic " + topic);
		}
		topicInfo.put(partition, isEligible);
	}

	private boolean isPartitionEligible(String topic, int partition) {
		Map<Integer, Boolean> topicInfo = isPartitionEligibleForWM.get(topic);
		if(topicInfo == null) {
			throw new RuntimeException("Unknown Topic " + topic);
		}
		Boolean isEligible = topicInfo.get(partition);
		return (isEligible != null) & isEligible;
	}

	/**
	 * Returns a map with the maximum timestamps seen per partition of the given topic.
	 * @param topic
	 *           The topic we are interested in.
	 * */
	private Map<Integer, Long> getMaxTimestampsForPartitionsOfTopic(String topic) {
		Map<Integer, Long> maxTimestampsForTopic = maxTsPerTopicAndPartition.get(topic);
		if(maxTimestampsForTopic == null) {
			maxTimestampsForTopic = new HashMap<>();
			maxTsPerTopicAndPartition.put(topic, maxTimestampsForTopic);
			isPartitionEligibleForWM.put(topic, new HashMap<Integer, Boolean>());
		}
		return maxTimestampsForTopic;
	}
}
