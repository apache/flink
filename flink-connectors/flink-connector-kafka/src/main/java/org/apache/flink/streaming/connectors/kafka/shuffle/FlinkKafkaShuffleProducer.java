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

package org.apache.flink.streaming.connectors.kafka.shuffle;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaException;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.PropertiesUtil;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.shuffle.FlinkKafkaShuffle.PARTITION_NUMBER;

/**
 * Flink Kafka Shuffle Producer Function.
 * It is different from {@link FlinkKafkaProducer} in the way handling elements and watermarks
 */
@Internal
public class FlinkKafkaShuffleProducer<IN, KEY> extends FlinkKafkaProducer<IN> {
	private final KafkaSerializer<IN> kafkaSerializer;
	private final KeySelector<IN, KEY> keySelector;
	private final int numberOfPartitions;

	FlinkKafkaShuffleProducer(
			String defaultTopicId,
			TypeSerializer<IN> typeSerializer,
			Properties props,
			KeySelector<IN, KEY> keySelector,
			Semantic semantic,
			int kafkaProducersPoolSize) {
		super(defaultTopicId, (element, timestamp) -> null, props, semantic, kafkaProducersPoolSize);

		this.kafkaSerializer = new KafkaSerializer<>(typeSerializer);
		this.keySelector = keySelector;

		Preconditions.checkArgument(
			props.getProperty(PARTITION_NUMBER) != null,
			"Missing partition number for Kafka Shuffle");
		numberOfPartitions = PropertiesUtil.getInt(props, PARTITION_NUMBER, Integer.MIN_VALUE);
	}

	/**
	 * This is the function invoked to handle each element.
	 *
	 * @param transaction Transaction state;
	 *                    elements are written to Kafka in transactions to guarantee different level of data consistency
	 * @param next Element to handle
	 * @param context Context needed to handle the element
	 * @throws FlinkKafkaException for kafka error
	 */
	@Override
	public void invoke(KafkaTransactionState transaction, IN next, Context context) throws FlinkKafkaException {
		checkErroneous();

		// write timestamp to Kafka if timestamp is available
		Long timestamp = context.timestamp();

		int[] partitions = getPartitions(transaction);
		int partitionIndex;
		try {
			partitionIndex = KeyGroupRangeAssignment
				.assignKeyToParallelOperator(keySelector.getKey(next), partitions.length, partitions.length);
		} catch (Exception e) {
			throw new RuntimeException("Fail to assign a partition number to record", e);
		}

		ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
			defaultTopicId,
			partitionIndex,
			timestamp,
			null,
			kafkaSerializer.serializeRecord(next, timestamp));

		pendingRecords.incrementAndGet();
		transaction.getProducer().send(record, callback);
	}

	/**
	 * This is the function invoked to handle each watermark.
	 *
	 * @param watermark Watermark to handle
	 * @throws FlinkKafkaException For kafka error
	 */
	public void invoke(Watermark watermark) throws FlinkKafkaException {
		checkErroneous();
		KafkaTransactionState transaction = currentTransaction();

		int[] partitions = getPartitions(transaction);
		int subtask = getRuntimeContext().getIndexOfThisSubtask();

		// broadcast watermark
		long timestamp = watermark.getTimestamp();
		for (int partition : partitions) {
			ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
				defaultTopicId,
				partition,
				timestamp,
				null,
				kafkaSerializer.serializeWatermark(watermark, subtask));

			pendingRecords.incrementAndGet();
			transaction.getProducer().send(record, callback);
		}
	}

	private int[] getPartitions(KafkaTransactionState transaction) {
		int[] partitions = topicPartitionsMap.get(defaultTopicId);
		if (partitions == null) {
			partitions = getPartitionsByTopic(defaultTopicId, transaction.getProducer());
			topicPartitionsMap.put(defaultTopicId, partitions);
		}

		Preconditions.checkArgument(partitions.length == numberOfPartitions);

		return partitions;
	}

	/**
	 * Flink Kafka Shuffle Serializer.
	 */
	public static final class KafkaSerializer<IN> implements Serializable {
		public static final int TAG_REC_WITH_TIMESTAMP = 0;
		public static final int TAG_REC_WITHOUT_TIMESTAMP = 1;
		public static final int TAG_WATERMARK = 2;

		private static final long serialVersionUID = 2000002L;
		// easy for updating SerDe format later
		private static final int KAFKA_SHUFFLE_VERSION = 0;

		private final TypeSerializer<IN> serializer;

		private transient DataOutputSerializer dos;

		KafkaSerializer(TypeSerializer<IN> serializer) {
			this.serializer = serializer;
		}

		/**
		 * Format: Version(byte), TAG(byte), [timestamp(long)], record.
		 */
		byte[] serializeRecord(IN record, Long timestamp) {
			if (dos == null) {
				dos = new DataOutputSerializer(16);
			}

			try {
				dos.write(KAFKA_SHUFFLE_VERSION);

				if (timestamp == null) {
					dos.write(TAG_REC_WITHOUT_TIMESTAMP);
				} else {
					dos.write(TAG_REC_WITH_TIMESTAMP);
					dos.writeLong(timestamp);
				}
				serializer.serialize(record, dos);

			} catch (IOException e) {
				throw new RuntimeException("Unable to serialize record", e);
			}

			byte[] ret = dos.getCopyOfBuffer();
			dos.clear();
			return ret;
		}

		/**
		 * Format: Version(byte), TAG(byte), subtask(int), timestamp(long).
		 */
		byte[] serializeWatermark(Watermark watermark, int subtask) {
			if (dos == null) {
				dos = new DataOutputSerializer(16);
			}

			try {
				dos.write(KAFKA_SHUFFLE_VERSION);
				dos.write(TAG_WATERMARK);
				dos.writeInt(subtask);
				dos.writeLong(watermark.getTimestamp());
			} catch (IOException e) {
				throw new RuntimeException("Unable to serialize watermark", e);
			}

			byte[] ret = dos.getCopyOfBuffer();
			dos.clear();
			return ret;
		}
	}
}
