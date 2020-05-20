/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.COMMITTED_OFFSETS_METRICS_GAUGE;
import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.CURRENT_OFFSETS_METRICS_GAUGE;
import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.LEGACY_COMMITTED_OFFSETS_METRICS_GROUP;
import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.LEGACY_CURRENT_OFFSETS_METRICS_GROUP;
import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.OFFSETS_BY_PARTITION_METRICS_GROUP;
import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.OFFSETS_BY_TOPIC_METRICS_GROUP;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for all fetchers, which implement the connections to Kafka brokers and
 * pull records from Kafka partitions.
 *
 * <p>This fetcher base class implements the logic around emitting records and tracking offsets,
 * as well as around the optional timestamp assignment and watermark generation.
 *
 * @param <T> The type of elements deserialized from Kafka's byte records, and emitted into
 *            the Flink data streams.
 * @param <KPH> The type of topic/partition identifier used by Kafka in the specific version.
 */
@Internal
public abstract class AbstractFetcher<T, KPH> {

	private static final int NO_TIMESTAMPS_WATERMARKS = 0;
	private static final int WITH_WATERMARK_GENERATOR = 1;

	// ------------------------------------------------------------------------

	/** The source context to emit records and watermarks to. */
	protected final SourceContext<T> sourceContext;

	/**
	 * Wrapper around our SourceContext for allowing the {@link org.apache.flink.api.common.eventtime.WatermarkGenerator}
	 * to emit watermarks and mark idleness.
	 */
	protected final WatermarkOutput watermarkOutput;

	/**
	 * {@link WatermarkOutputMultiplexer} for supporting per-partition watermark generation.
	 */
	private final WatermarkOutputMultiplexer watermarkOutputMultiplexer;

	/** The lock that guarantees that record emission and state updates are atomic,
	 * from the view of taking a checkpoint. */
	protected final Object checkpointLock;

	/** All partitions (and their state) that this fetcher is subscribed to. */
	private final List<KafkaTopicPartitionState<T, KPH>> subscribedPartitionStates;

	/**
	 * Queue of partitions that are not yet assigned to any Kafka clients for consuming.
	 * Kafka version-specific implementations of {@link AbstractFetcher#runFetchLoop()}
	 * should continuously poll this queue for unassigned partitions, and start consuming
	 * them accordingly.
	 *
	 * <p>All partitions added to this queue are guaranteed to have been added
	 * to {@link #subscribedPartitionStates} already.
	 */
	protected final ClosableBlockingQueue<KafkaTopicPartitionState<T, KPH>> unassignedPartitionsQueue;

	/** The mode describing whether the fetcher also generates timestamps and watermarks. */
	private final int timestampWatermarkMode;

	/**
	 * Optional watermark strategy that will be run per Kafka partition, to exploit per-partition
	 * timestamp characteristics. The watermark strategy is kept in serialized form, to deserialize
	 * it into multiple copies.
	 */
	private final SerializedValue<WatermarkStrategy<T>> watermarkStrategy;

	/** User class loader used to deserialize watermark assigners. */
	private final ClassLoader userCodeClassLoader;

	// ------------------------------------------------------------------------
	//  Metrics
	// ------------------------------------------------------------------------

	/**
	 * Flag indicating whether or not metrics should be exposed.
	 * If {@code true}, offset metrics (e.g. current offset, committed offset) and
	 * Kafka-shipped metrics will be registered.
	 */
	private final boolean useMetrics;

	/**
	 * The metric group which all metrics for the consumer should be registered to.
	 * This metric group is defined under the user scope {@link KafkaConsumerMetricConstants#KAFKA_CONSUMER_METRICS_GROUP}.
	 */
	private final MetricGroup consumerMetricGroup;

	@SuppressWarnings("DeprecatedIsStillUsed")
	@Deprecated
	private final MetricGroup legacyCurrentOffsetsMetricGroup;

	@SuppressWarnings("DeprecatedIsStillUsed")
	@Deprecated
	private final MetricGroup legacyCommittedOffsetsMetricGroup;

	protected AbstractFetcher(
			SourceContext<T> sourceContext,
			Map<KafkaTopicPartition, Long> seedPartitionsWithInitialOffsets,
			SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
			ProcessingTimeService processingTimeProvider,
			long autoWatermarkInterval,
			ClassLoader userCodeClassLoader,
			MetricGroup consumerMetricGroup,
			boolean useMetrics) throws Exception {
		this.sourceContext = checkNotNull(sourceContext);
		this.watermarkOutput = new SourceContextWatermarkOutputAdapter<>(sourceContext);
		this.watermarkOutputMultiplexer = new WatermarkOutputMultiplexer(watermarkOutput);
		this.checkpointLock = sourceContext.getCheckpointLock();
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);

		this.useMetrics = useMetrics;
		this.consumerMetricGroup = checkNotNull(consumerMetricGroup);
		this.legacyCurrentOffsetsMetricGroup = consumerMetricGroup.addGroup(LEGACY_CURRENT_OFFSETS_METRICS_GROUP);
		this.legacyCommittedOffsetsMetricGroup = consumerMetricGroup.addGroup(LEGACY_COMMITTED_OFFSETS_METRICS_GROUP);

		this.watermarkStrategy = watermarkStrategy;

		if (watermarkStrategy == null) {
			timestampWatermarkMode = NO_TIMESTAMPS_WATERMARKS;
		} else {
			timestampWatermarkMode = WITH_WATERMARK_GENERATOR;
		}

		this.unassignedPartitionsQueue = new ClosableBlockingQueue<>();

		// initialize subscribed partition states with seed partitions
		this.subscribedPartitionStates = createPartitionStateHolders(
				seedPartitionsWithInitialOffsets,
				timestampWatermarkMode,
				watermarkStrategy,
				userCodeClassLoader);

		// check that all seed partition states have a defined offset
		for (KafkaTopicPartitionState<?, ?> partitionState : subscribedPartitionStates) {
			if (!partitionState.isOffsetDefined()) {
				throw new IllegalArgumentException("The fetcher was assigned seed partitions with undefined initial offsets.");
			}
		}

		// all seed partitions are not assigned yet, so should be added to the unassigned partitions queue
		for (KafkaTopicPartitionState<T, KPH> partition : subscribedPartitionStates) {
			unassignedPartitionsQueue.add(partition);
		}

		// register metrics for the initial seed partitions
		if (useMetrics) {
			registerOffsetMetrics(consumerMetricGroup, subscribedPartitionStates);
		}

		// if we have periodic watermarks, kick off the interval scheduler
		if (timestampWatermarkMode == WITH_WATERMARK_GENERATOR && autoWatermarkInterval > 0) {
			PeriodicWatermarkEmitter<T, KPH> periodicEmitter = new PeriodicWatermarkEmitter<>(
					checkpointLock,
					subscribedPartitionStates,
					watermarkOutputMultiplexer,
					processingTimeProvider,
					autoWatermarkInterval);

			periodicEmitter.start();
		}
	}

	/**
	 * Adds a list of newly discovered partitions to the fetcher for consuming.
	 *
	 * <p>This method creates the partition state holder for each new partition, using
	 * {@link KafkaTopicPartitionStateSentinel#EARLIEST_OFFSET} as the starting offset.
	 * It uses the earliest offset because there may be delay in discovering a partition
	 * after it was created and started receiving records.
	 *
	 * <p>After the state representation for a partition is created, it is added to the
	 * unassigned partitions queue to await to be consumed.
	 *
	 * @param newPartitions discovered partitions to add
	 */
	public void addDiscoveredPartitions(List<KafkaTopicPartition> newPartitions) throws IOException, ClassNotFoundException {
		List<KafkaTopicPartitionState<T, KPH>> newPartitionStates = createPartitionStateHolders(
				newPartitions,
				KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET,
				timestampWatermarkMode,
				watermarkStrategy,
				userCodeClassLoader);

		if (useMetrics) {
			registerOffsetMetrics(consumerMetricGroup, newPartitionStates);
		}

		for (KafkaTopicPartitionState<T, KPH> newPartitionState : newPartitionStates) {
			// the ordering is crucial here; first register the state holder, then
			// push it to the partitions queue to be read
			subscribedPartitionStates.add(newPartitionState);
			unassignedPartitionsQueue.add(newPartitionState);
		}
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets all partitions (with partition state) that this fetcher is subscribed to.
	 *
	 * @return All subscribed partitions.
	 */
	protected final List<KafkaTopicPartitionState<T, KPH>> subscribedPartitionStates() {
		return subscribedPartitionStates;
	}

	// ------------------------------------------------------------------------
	//  Core fetcher work methods
	// ------------------------------------------------------------------------

	public abstract void runFetchLoop() throws Exception;

	public abstract void cancel();

	// ------------------------------------------------------------------------
	//  Kafka version specifics
	// ------------------------------------------------------------------------

	/**
	 * Commits the given partition offsets to the Kafka brokers (or to ZooKeeper for
	 * older Kafka versions). This method is only ever called when the offset commit mode of
	 * the consumer is {@link OffsetCommitMode#ON_CHECKPOINTS}.
	 *
	 * <p>The given offsets are the internal checkpointed offsets, representing
	 * the last processed record of each partition. Version-specific implementations of this method
	 * need to hold the contract that the given offsets must be incremented by 1 before
	 * committing them, so that committed offsets to Kafka represent "the next record to process".
	 *
	 * @param offsets The offsets to commit to Kafka (implementations must increment offsets by 1 before committing).
	 * @param commitCallback The callback that the user should trigger when a commit request completes or fails.
	 * @throws Exception This method forwards exceptions.
	 */
	public final void commitInternalOffsetsToKafka(
			Map<KafkaTopicPartition, Long> offsets,
			@Nonnull KafkaCommitCallback commitCallback) throws Exception {
		// Ignore sentinels. They might appear here if snapshot has started before actual offsets values
		// replaced sentinels
		doCommitInternalOffsetsToKafka(filterOutSentinels(offsets), commitCallback);
	}

	protected abstract void doCommitInternalOffsetsToKafka(
			Map<KafkaTopicPartition, Long> offsets,
			@Nonnull KafkaCommitCallback commitCallback) throws Exception;

	private Map<KafkaTopicPartition, Long> filterOutSentinels(Map<KafkaTopicPartition, Long> offsets) {
		return offsets.entrySet()
			.stream()
			.filter(entry -> !KafkaTopicPartitionStateSentinel.isSentinel(entry.getValue()))
			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	/**
	 * Creates the Kafka version specific representation of the given
	 * topic partition.
	 *
	 * @param partition The Flink representation of the Kafka topic partition.
	 * @return The version-specific Kafka representation of the Kafka topic partition.
	 */
	protected abstract KPH createKafkaPartitionHandle(KafkaTopicPartition partition);

	// ------------------------------------------------------------------------
	//  snapshot and restore the state
	// ------------------------------------------------------------------------

	/**
	 * Takes a snapshot of the partition offsets.
	 *
	 * <p>Important: This method must be called under the checkpoint lock.
	 *
	 * @return A map from partition to current offset.
	 */
	public HashMap<KafkaTopicPartition, Long> snapshotCurrentState() {
		// this method assumes that the checkpoint lock is held
		assert Thread.holdsLock(checkpointLock);

		HashMap<KafkaTopicPartition, Long> state = new HashMap<>(subscribedPartitionStates.size());
		for (KafkaTopicPartitionState<T, KPH> partition : subscribedPartitionStates) {
			state.put(partition.getKafkaTopicPartition(), partition.getOffset());
		}
		return state;
	}

	// ------------------------------------------------------------------------
	//  emitting records
	// ------------------------------------------------------------------------

	/**
	 * Emits a record attaching a timestamp to it.
	 *  @param records The records to emit
	 * @param partitionState The state of the Kafka partition from which the record was fetched
	 * @param offset The offset of the corresponding Kafka record
	 * @param kafkaEventTimestamp The timestamp of the Kafka record
	 */
	protected void emitRecordsWithTimestamps(
			Queue<T> records,
			KafkaTopicPartitionState<T, KPH> partitionState,
			long offset,
			long kafkaEventTimestamp) {
		// emit the records, using the checkpoint lock to guarantee
		// atomicity of record emission and offset state update
		synchronized (checkpointLock) {
			T record;
			while ((record = records.poll()) != null) {
				long timestamp = partitionState.extractTimestamp(record, kafkaEventTimestamp);
				sourceContext.collectWithTimestamp(record, timestamp);

				// this might emit a watermark, so do it after emitting the record
				partitionState.onEvent(record, timestamp);
			}
			partitionState.setOffset(offset);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Utility method that takes the topic partitions and creates the topic partition state
	 * holders, depending on the timestamp / watermark mode.
	 */
	private List<KafkaTopicPartitionState<T, KPH>> createPartitionStateHolders(
			Map<KafkaTopicPartition, Long> partitionsToInitialOffsets,
			int timestampWatermarkMode,
			SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
			ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {

		// CopyOnWrite as adding discovered partitions could happen in parallel
		// while different threads iterate the partitions list
		List<KafkaTopicPartitionState<T, KPH>> partitionStates = new CopyOnWriteArrayList<>();

		switch (timestampWatermarkMode) {
			case NO_TIMESTAMPS_WATERMARKS: {
				for (Map.Entry<KafkaTopicPartition, Long> partitionEntry : partitionsToInitialOffsets.entrySet()) {
					// create the kafka version specific partition handle
					KPH kafkaHandle = createKafkaPartitionHandle(partitionEntry.getKey());

					KafkaTopicPartitionState<T, KPH> partitionState =
							new KafkaTopicPartitionState<>(partitionEntry.getKey(), kafkaHandle);
					partitionState.setOffset(partitionEntry.getValue());

					partitionStates.add(partitionState);
				}

				return partitionStates;
			}

			case WITH_WATERMARK_GENERATOR: {
				for (Map.Entry<KafkaTopicPartition, Long> partitionEntry : partitionsToInitialOffsets.entrySet()) {
					KPH kafkaHandle = createKafkaPartitionHandle(partitionEntry.getKey());
					WatermarkStrategy<T> deserializedWatermarkStrategy = watermarkStrategy.deserializeValue(
							userCodeClassLoader);

					int outputId = watermarkOutputMultiplexer.registerNewOutput();
					WatermarkOutput immediateOutput =
							watermarkOutputMultiplexer.getImmediateOutput(outputId);
					WatermarkOutput deferredOutput =
							watermarkOutputMultiplexer.getDeferredOutput(outputId);

					KafkaTopicPartitionStateWithWatermarkGenerator<T, KPH> partitionState =
							new KafkaTopicPartitionStateWithWatermarkGenerator<>(
									partitionEntry.getKey(),
									kafkaHandle,
									deserializedWatermarkStrategy.createTimestampAssigner(() -> consumerMetricGroup),
									deserializedWatermarkStrategy.createWatermarkGenerator(() -> consumerMetricGroup),
									immediateOutput,
									deferredOutput);

					partitionState.setOffset(partitionEntry.getValue());

					partitionStates.add(partitionState);
				}

				return partitionStates;
			}

			default:
				// cannot happen, add this as a guard for the future
				throw new RuntimeException();
		}
	}

	/**
	 * Shortcut variant of {@link #createPartitionStateHolders(Map, int, SerializedValue, ClassLoader)}
	 * that uses the same offset for all partitions when creating their state holders.
	 */
	private List<KafkaTopicPartitionState<T, KPH>> createPartitionStateHolders(
		List<KafkaTopicPartition> partitions,
		long initialOffset,
		int timestampWatermarkMode,
		SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
		ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {

		Map<KafkaTopicPartition, Long> partitionsToInitialOffset = new HashMap<>(partitions.size());
		for (KafkaTopicPartition partition : partitions) {
			partitionsToInitialOffset.put(partition, initialOffset);
		}

		return createPartitionStateHolders(
				partitionsToInitialOffset,
				timestampWatermarkMode,
				watermarkStrategy,
				userCodeClassLoader);
	}

	// ------------------------- Metrics ----------------------------------

	/**
	 * For each partition, register a new metric group to expose current offsets and committed offsets.
	 * Per-partition metric groups can be scoped by user variables {@link KafkaConsumerMetricConstants#OFFSETS_BY_TOPIC_METRICS_GROUP}
	 * and {@link KafkaConsumerMetricConstants#OFFSETS_BY_PARTITION_METRICS_GROUP}.
	 *
	 * <p>Note: this method also registers gauges for deprecated offset metrics, to maintain backwards compatibility.
	 *
	 * @param consumerMetricGroup The consumer metric group
	 * @param partitionOffsetStates The partition offset state holders, whose values will be used to update metrics
	 */
	private void registerOffsetMetrics(
			MetricGroup consumerMetricGroup,
			List<KafkaTopicPartitionState<T, KPH>> partitionOffsetStates) {

		for (KafkaTopicPartitionState<T, KPH> ktp : partitionOffsetStates) {
			MetricGroup topicPartitionGroup = consumerMetricGroup
				.addGroup(OFFSETS_BY_TOPIC_METRICS_GROUP, ktp.getTopic())
				.addGroup(OFFSETS_BY_PARTITION_METRICS_GROUP, Integer.toString(ktp.getPartition()));

			topicPartitionGroup.gauge(CURRENT_OFFSETS_METRICS_GAUGE, new OffsetGauge(ktp, OffsetGaugeType.CURRENT_OFFSET));
			topicPartitionGroup.gauge(COMMITTED_OFFSETS_METRICS_GAUGE, new OffsetGauge(ktp, OffsetGaugeType.COMMITTED_OFFSET));

			legacyCurrentOffsetsMetricGroup.gauge(getLegacyOffsetsMetricsGaugeName(ktp), new OffsetGauge(ktp, OffsetGaugeType.CURRENT_OFFSET));
			legacyCommittedOffsetsMetricGroup.gauge(getLegacyOffsetsMetricsGaugeName(ktp), new OffsetGauge(ktp, OffsetGaugeType.COMMITTED_OFFSET));
		}
	}

	private static String getLegacyOffsetsMetricsGaugeName(KafkaTopicPartitionState<?, ?> ktp) {
		return ktp.getTopic() + "-" + ktp.getPartition();
	}

	/**
	 * Gauge types.
	 */
	private enum OffsetGaugeType {
		CURRENT_OFFSET,
		COMMITTED_OFFSET
	}

	/**
	 * Gauge for getting the offset of a KafkaTopicPartitionState.
	 */
	private static class OffsetGauge implements Gauge<Long> {

		private final KafkaTopicPartitionState<?, ?> ktp;
		private final OffsetGaugeType gaugeType;

		OffsetGauge(KafkaTopicPartitionState<?, ?> ktp, OffsetGaugeType gaugeType) {
			this.ktp = ktp;
			this.gaugeType = gaugeType;
		}

		@Override
		public Long getValue() {
			switch(gaugeType) {
				case COMMITTED_OFFSET:
					return ktp.getCommittedOffset();
				case CURRENT_OFFSET:
					return ktp.getOffset();
				default:
					throw new RuntimeException("Unknown gauge type: " + gaugeType);
			}
		}
	}
 	// ------------------------------------------------------------------------

	/**
	 * The periodic watermark emitter. In its given interval, it checks all partitions for
	 * the current event time watermark, and possibly emits the next watermark.
	 */
	private static class PeriodicWatermarkEmitter<T, KPH> implements ProcessingTimeCallback {

		private final Object checkpointLock;

		private final List<KafkaTopicPartitionState<T, KPH>> allPartitions;

		private final WatermarkOutputMultiplexer watermarkOutputMultiplexer;

		private final ProcessingTimeService timerService;

		private final long interval;

		//-------------------------------------------------

		PeriodicWatermarkEmitter(
				Object checkpointLock,
				List<KafkaTopicPartitionState<T, KPH>> allPartitions,
				WatermarkOutputMultiplexer watermarkOutputMultiplexer,
				ProcessingTimeService timerService,
				long autoWatermarkInterval) {
			this.checkpointLock = checkpointLock;
			this.allPartitions = checkNotNull(allPartitions);
			this.watermarkOutputMultiplexer = watermarkOutputMultiplexer;
			this.timerService = checkNotNull(timerService);
			this.interval = autoWatermarkInterval;
		}

		//-------------------------------------------------

		public void start() {
			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
		}

		@Override
		public void onProcessingTime(long timestamp) {

			synchronized (checkpointLock) {
				for (KafkaTopicPartitionState<?, ?> state : allPartitions) {
					state.onPeriodicEmit();
				}

				watermarkOutputMultiplexer.onPeriodicEmit();
			}

			// schedule the next watermark
			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
		}
	}
}
