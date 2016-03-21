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

/**
 * This class adds functionality to allow Kafka consumers to emit watermarks based on their input data.
 * If the Kafka source has multiple partitions per source task, then the records become out of order before
 * timestamps can be extracted and watermarks can be generated. This class allows to keep track of the maximum
 * timestamp seen per partition, and emit watermarks (either periodically or in a punctuated way) with timestamp
 * equal to minimum, of these maximum timestamps seen across all local topics and partitions.
 *
 * The decision if this is going to be done periodically or in a punctuated way depends on the user provided
 * {@link org.apache.flink.streaming.api.functions.TimestampAssigner}. For periodic watermark emission, the user
 * has to provide a {@link org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks}, while for
 * punctuated ones, a {@link org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks}.
 */
public abstract class FlinkKafkaComsumerWithWMBase<T> extends FlinkKafkaConsumerBase<T> {


	/** Keeps track of the minimum timestamp seen, per Kafka topic per partition */
	private final Map<String, Map<Integer, Long>> maxTsPerTopicAndPartition = new HashMap<String, Map<Integer, Long>>();

	/**
	 * 	Keeps track of which flags are active, i.e. still receive fresh data.
	 * 	This flag is used to avoid the scenario where (event) time does not advance, and watermarks are not
	 * 	emitted after one of the partitions receives no more data. In this case, the inactive partition is
	 * 	ignored and the timestamp of the watermark is decided based on the remaining ones.
	 */
	private final Map<String, Map<Integer, Boolean>> isPartitionActive = new HashMap<>();

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
	 * Emits a new watermark, with timestamp equal to the minimum timestamp seen across
	 * all local topics and partitions. The new watermark is emitted if and only if it signals progress
	 * in event-time, i.e. if its timestamp is greater than the timestamp of the last emitted
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

	/**
	 * Returns the minimum timestamp seen across ALL topics and ALL local partitions,
	 * along with the topic and the partition the timestamp belongs to. This corresponds to
	 * the minimum across all the max timestamps seen per partition, per topic.
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
	 * Returns the minimum timestamp for a topic among the max timestamps seen for each of its partitions, along
	 * with the partition it belongs to.
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
	 * Sets the {@code active} flag for a given topic and partition to {@code isEligible}.
	 * This flag is used to signal if the partition is still active and it still receives data.
	 * */
	private void setEligibleFlagForPartition(String topic, int partition, boolean isActive) {
		Map<Integer, Boolean> topicInfo = isPartitionActive.get(topic);
		if(topicInfo == null) {
			throw new RuntimeException("Unknown Topic " + topic);
		}
		topicInfo.put(partition, isActive);
	}

	/**
	 * Checks if a partition of a topic is still active, i.e. if it still receives data.
	 * @param topic
	 *          The topic of the partition.
	 * @param partition
	 *          The partition to be checked.
	 * */
	private boolean isPartitionEligible(String topic, int partition) {
		Map<Integer, Boolean> topicInfo = isPartitionActive.get(topic);
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
			isPartitionActive.put(topic, new HashMap<Integer, Boolean>());
		}
		return maxTimestampsForTopic;
	}
}
