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

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.shuffle.FlinkKafkaShuffleProducer.KafkaSerializer.TAG_REC_WITHOUT_TIMESTAMP;
import static org.apache.flink.streaming.connectors.kafka.shuffle.FlinkKafkaShuffleProducer.KafkaSerializer.TAG_REC_WITH_TIMESTAMP;
import static org.apache.flink.streaming.connectors.kafka.shuffle.FlinkKafkaShuffleProducer.KafkaSerializer.TAG_WATERMARK;

/**
 * Fetch data from Kafka for Kafka Shuffle.
 */
@Internal
public class KafkaShuffleFetcher<T> extends KafkaFetcher<T> {
	/** The handler to check and generate watermarks from fetched records. **/
	private final WatermarkHandler watermarkHandler;

	/** The schema to convert between Kafka's byte messages, and Flink's objects. */
	private final KafkaShuffleElementDeserializer kafkaShuffleDeserializer;

	public KafkaShuffleFetcher(
			SourceFunction.SourceContext<T> sourceContext,
			Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
			SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
			ProcessingTimeService processingTimeProvider,
			long autoWatermarkInterval,
			ClassLoader userCodeClassLoader,
			String taskNameWithSubtasks,
			KafkaDeserializationSchema<T> deserializer,
			Properties kafkaProperties,
			long pollTimeout,
			MetricGroup subtaskMetricGroup,
			MetricGroup consumerMetricGroup,
			boolean useMetrics,
			TypeSerializer<T> typeSerializer,
			int producerParallelism) throws Exception {
		super(
			sourceContext,
			assignedPartitionsWithInitialOffsets,
			watermarkStrategy,
			processingTimeProvider,
			autoWatermarkInterval,
			userCodeClassLoader,
			taskNameWithSubtasks,
			deserializer,
			kafkaProperties,
			pollTimeout,
			subtaskMetricGroup,
			consumerMetricGroup,
			useMetrics);

		this.kafkaShuffleDeserializer = new KafkaShuffleElementDeserializer<>(typeSerializer);
		this.watermarkHandler = new WatermarkHandler(producerParallelism);
	}

	@Override
	protected String getFetcherName() {
		return "Kafka Shuffle Fetcher";
	}

	@Override
	protected void partitionConsumerRecordsHandler(
			List<ConsumerRecord<byte[], byte[]>> partitionRecords,
			KafkaTopicPartitionState<T, TopicPartition> partition) throws Exception {

		for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
			final KafkaShuffleElement element = kafkaShuffleDeserializer.deserialize(record);

			// TODO: Do we need to check the end of stream if reaching the end watermark
			// TODO: Currently, if one of the partition sends an end-of-stream signal the fetcher stops running.
			// The current "ending of stream" logic in KafkaFetcher a bit strange: if any partition has a record
			// signaled as "END_OF_STREAM", the fetcher will stop running. Notice that the signal is coming from
			// the deserializer, which means from Kafka data itself. But it is possible that other topics
			// and partitions still have data to read. Finishing reading Partition0 can not guarantee that Partition1
			// also finishes.
			if (element.isRecord()) {
				// timestamp is inherent from upstream
				// If using ProcessTime, timestamp is going to be ignored (upstream does not include timestamp as well)
				// If using IngestionTime, timestamp is going to be overwritten
				// If using EventTime, timestamp is going to be used
				synchronized (checkpointLock) {
					KafkaShuffleRecord<T> elementAsRecord = element.asRecord();
					sourceContext.collectWithTimestamp(
						elementAsRecord.value,
						elementAsRecord.timestamp == null ? record.timestamp() : elementAsRecord.timestamp);
					partition.setOffset(record.offset());
				}
			} else if (element.isWatermark()) {
				final KafkaShuffleWatermark watermark = element.asWatermark();
				Optional<Watermark> newWatermark = watermarkHandler.checkAndGetNewWatermark(watermark);
				newWatermark.ifPresent(sourceContext::emitWatermark);
			}
		}
	}

	/**
	 * An element in a KafkaShuffle. Can be a record or a Watermark.
	 */
	@VisibleForTesting
	public abstract static class KafkaShuffleElement {

		public boolean isRecord() {
			return getClass() == KafkaShuffleRecord.class;
		}

		public boolean isWatermark() {
			return getClass() == KafkaShuffleWatermark.class;
		}

		public <T> KafkaShuffleRecord<T> asRecord() {
			return (KafkaShuffleRecord<T>) this;
		}

		public KafkaShuffleWatermark asWatermark() {
			return (KafkaShuffleWatermark) this;
		}
	}

	/**
	 * A watermark element in a KafkaShuffle. It includes
	 * - subtask index where the watermark is coming from
	 * - watermark timestamp
	 */
	@VisibleForTesting
	public static class KafkaShuffleWatermark extends KafkaShuffleElement {
		final int subtask;
		final long watermark;

		KafkaShuffleWatermark(int subtask, long watermark) {
			this.subtask = subtask;
			this.watermark = watermark;
		}

		public int getSubtask() {
			return subtask;
		}

		public long getWatermark() {
			return watermark;
		}
	}

	/**
	 * One value with Type T in a KafkaShuffle. This stores the value and an optional associated timestamp.
	 */
	@VisibleForTesting
	public static class KafkaShuffleRecord<T> extends KafkaShuffleElement {
		final T value;
		final Long timestamp;

		KafkaShuffleRecord(T value) {
			this.value = value;
			this.timestamp = null;
		}

		KafkaShuffleRecord(long timestamp, T value) {
			this.value = value;
			this.timestamp = timestamp;
		}

		public T getValue() {
			return value;
		}

		public Long getTimestamp() {
			return timestamp;
		}
	}

	/**
	 * Deserializer for KafkaShuffleElement.
	 */
	@VisibleForTesting
	public static class KafkaShuffleElementDeserializer<T> implements Serializable {
		private static final long serialVersionUID = 1000001L;

		private final TypeSerializer<T> typeSerializer;

		private transient DataInputDeserializer dis;

		@VisibleForTesting
		public KafkaShuffleElementDeserializer(TypeSerializer<T> typeSerializer) {
			this.typeSerializer = typeSerializer;
		}

		@VisibleForTesting
		public KafkaShuffleElement deserialize(ConsumerRecord<byte[], byte[]> record)
			throws Exception {
			byte[] value = record.value();

			if (dis != null) {
				dis.setBuffer(value);
			} else {
				dis = new DataInputDeserializer(value);
			}

			// version byte
			ByteSerializer.INSTANCE.deserialize(dis);
			int tag = ByteSerializer.INSTANCE.deserialize(dis);

			if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
				return new KafkaShuffleRecord<>(typeSerializer.deserialize(dis));
			} else if (tag == TAG_REC_WITH_TIMESTAMP) {
				return new KafkaShuffleRecord<>(
					LongSerializer.INSTANCE.deserialize(dis),
					typeSerializer.deserialize(dis));
			} else if (tag == TAG_WATERMARK) {
				return new KafkaShuffleWatermark(
					IntSerializer.INSTANCE.deserialize(dis), LongSerializer.INSTANCE.deserialize(dis));
			}

			throw new UnsupportedOperationException("Unsupported tag format");
		}
	}

	/**
	 * WatermarkHandler to check and generate watermarks from fetched records.
	 */
	private static class WatermarkHandler {
		private final int producerParallelism;
		private final Map<Integer, Long> subtaskWatermark;

		private long currentMinWatermark = Long.MIN_VALUE;

		WatermarkHandler(int producerParallelism) {
			this.producerParallelism = producerParallelism;
			this.subtaskWatermark = new HashMap<>(producerParallelism);
		}

		private Optional<Watermark> checkAndGetNewWatermark(KafkaShuffleWatermark newWatermark) {
			// watermarks is incremental for the same partition and PRODUCER subtask
			Long currentSubTaskWatermark = subtaskWatermark.get(newWatermark.subtask);

			// watermark is strictly increasing
			Preconditions.checkState(
				(currentSubTaskWatermark == null) || (currentSubTaskWatermark < newWatermark.watermark),
				"Watermark should always increase: current : new " + currentSubTaskWatermark + ":" + newWatermark.watermark);

			subtaskWatermark.put(newWatermark.subtask, newWatermark.watermark);

			if (subtaskWatermark.values().size() < producerParallelism) {
				return Optional.empty();
			}

			long minWatermark = subtaskWatermark.values().stream().min(Comparator.naturalOrder()).orElse(Long.MIN_VALUE);
			if (currentMinWatermark < minWatermark) {
				currentMinWatermark = minWatermark;
				return Optional.of(new Watermark(minWatermark));
			} else {
				return Optional.empty();
			}
		}
	}
}
