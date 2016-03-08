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

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

// TODO: 3/8/16 should I override the open() 
public class FlinkKafkaConsumer09WithPunctuatedWM<T> extends AbstractFlinkKafkaConsumer09<T> {

	/** Keeps the minimum timestamp seen, per topic per partition */
	private final Map<String, Map<Integer, Long>> minSeenTimestamps =
		new HashMap<String, Map<Integer, Long>>();

	private long lastEmittedWatermark = Long.MIN_VALUE;

	protected AssignerWithPunctuatedWatermarks<T> punctuatedWatermarkAssigner;

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.9.x
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer09WithPunctuatedWM(String topic, DeserializationSchema<T> valueDeserializer, Properties props,
												AssignerWithPunctuatedWatermarks<T> timestampAssigner) {
		this(Collections.singletonList(topic), valueDeserializer, props, timestampAssigner);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.9.x
	 *
	 * This constructor allows passing a {@see KeyedDeserializationSchema} for reading key/value
	 * pairs, offsets, and topic names from Kafka.
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer09WithPunctuatedWM(String topic, KeyedDeserializationSchema<T> deserializer, Properties props,
												AssignerWithPunctuatedWatermarks<T> timestampAssigner) {
		super(Collections.singletonList(topic), deserializer, props);
		this.punctuatedWatermarkAssigner = timestampAssigner;
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.9.x
	 *
	 * This constructor allows passing multiple topics to the consumer.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkKafkaConsumer09WithPunctuatedWM(List<String> topics, DeserializationSchema<T> deserializer, Properties props,
												AssignerWithPunctuatedWatermarks<T> timestampAssigner) {
		super(topics, new KeyedDeserializationSchemaWrapper<>(deserializer), props);
		this.punctuatedWatermarkAssigner = timestampAssigner;
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.9.x
	 *
	 * This constructor allows passing multiple topics and a key/value deserialization schema.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkKafkaConsumer09WithPunctuatedWM(List<String> topics, KeyedDeserializationSchema<T> deserializer, Properties props,
												AssignerWithPunctuatedWatermarks<T> timestampAssigner) {
		super(topics, deserializer, props);
		this.punctuatedWatermarkAssigner = timestampAssigner;
	}

	@Override
	public void processElement(SourceContext<T> sourceContext, TopicPartition partitionInfo, T value) {
		// extract the timestamp based on the user-specified extractor
		// and emit the element with the new timestamp
		long extractedTimestamp = this.punctuatedWatermarkAssigner.extractTimestamp(value, Long.MIN_VALUE);
		sourceContext.collectWithTimestamp(value, extractedTimestamp);

		// update the list of minimum timestamps seen per partition
		boolean updated = updateMinimumTimestampForPartition(partitionInfo, extractedTimestamp);

		// get the minimum timestamp for the topic across all local partitions
		long minTimestampForTopic = getMinimumTimestampForTopic(partitionInfo);

		// if the user specified extractor allows it and the extracted timestamp has a later timestamp
		// than the last emitted watermark for the topic, emit a new watermark to signal event-time progress
		final Watermark nextWatermark = this.punctuatedWatermarkAssigner.checkAndGetNextWatermark(value, minTimestampForTopic);
		if (nextWatermark != null && nextWatermark.getTimestamp() > lastEmittedWatermark) {
			lastEmittedWatermark = nextWatermark.getTimestamp();
			sourceContext.emitWatermark(nextWatermark);
		}
	}


	private Map<Integer, Long> getMinTimestampsPerPartitionForTopic(String topic) {
		Map<Integer, Long> minTimestampsForTopic = this.minSeenTimestamps.get(topic);
		if(minTimestampsForTopic == null) {
			LOG.info("Tracking timestamps for topic: " + topic);
			this.minSeenTimestamps.put(topic, new HashMap<Integer, Long>());
		}
		return minTimestampsForTopic;
	}

	private boolean updateMinimumTimestampForPartition(TopicPartition partitionInfo, long timestamp) {
		Map<Integer, Long> minTimestampsForTopic =
			getMinTimestampsPerPartitionForTopic(partitionInfo.topic());

		Integer partition = partitionInfo.partition();
		Long minTimestampForPartition = minTimestampsForTopic.get(partition);
		if(minTimestampForPartition == null || timestamp < minTimestampForPartition) {
			minTimestampsForTopic.put(partition, timestamp);
			return true;
		}
		return false;
	}

	private long getMinimumTimestampForTopic(TopicPartition partitionInfo) {
		Map<Integer, Long> minTimestampsForTopic =
			getMinTimestampsPerPartitionForTopic(partitionInfo.topic());

		long minTimestamp = Long.MAX_VALUE;
		for(Long ts: minTimestampsForTopic.values()){
			if(ts < minTimestamp) {
				minTimestamp = ts;
			}
		}
		return minTimestamp == Long.MAX_VALUE ? Long.MIN_VALUE : minTimestamp;
	}
}
