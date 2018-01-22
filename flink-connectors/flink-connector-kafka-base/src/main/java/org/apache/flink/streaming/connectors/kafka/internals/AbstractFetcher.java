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
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
	private static final int PERIODIC_WATERMARKS = 1;
	private static final int PUNCTUATED_WATERMARKS = 2;

	// ------------------------------------------------------------------------

	/** The source context to emit records and watermarks to. */
	protected final SourceContext<T> sourceContext;

	/** The lock that guarantees that record emission and state updates are atomic,
	 * from the view of taking a checkpoint. */
	private final Object checkpointLock;

	/** All partitions (and their state) that this fetcher is subscribed to. */
	private final List<KafkaTopicPartitionState<KPH>> subscribedPartitionStates;

	/**
	 * Queue of partitions that are not yet assigned to any Kafka clients for consuming.
	 * Kafka version-specific implementations of {@link AbstractFetcher#runFetchLoop()}
	 * should continuously poll this queue for unassigned partitions, and start consuming
	 * them accordingly.
	 *
	 * <p>All partitions added to this queue are guaranteed to have been added
	 * to {@link #subscribedPartitionStates} already.
	 */
	protected final ClosableBlockingQueue<KafkaTopicPartitionState<KPH>> unassignedPartitionsQueue;

	/** The mode describing whether the fetcher also generates timestamps and watermarks. */
	private final int timestampWatermarkMode;

	/**
	 * Optional timestamp extractor / watermark generator that will be run per Kafka partition,
	 * to exploit per-partition timestamp characteristics.
	 * The assigner is kept in serialized form, to deserialize it into multiple copies.
	 */
	private final SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic;

	/**
	 * Optional timestamp extractor / watermark generator that will be run per Kafka partition,
	 * to exploit per-partition timestamp characteristics.
	 * The assigner is kept in serialized form, to deserialize it into multiple copies.
	 */
	private final SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated;

	/** User class loader used to deserialize watermark assigners. */
	private final ClassLoader userCodeClassLoader;

	/** Only relevant for punctuated watermarks: The current cross partition watermark. */
	private volatile long maxWatermarkSoFar = Long.MIN_VALUE;

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

	@Deprecated
	private final MetricGroup legacyCurrentOffsetsMetricGroup;

	@Deprecated
	private final MetricGroup legacyCommittedOffsetsMetricGroup;

	protected AbstractFetcher(
			SourceContext<T> sourceContext,
			Map<KafkaTopicPartition, Long> seedPartitionsWithInitialOffsets,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			ProcessingTimeService processingTimeProvider,
			long autoWatermarkInterval,
			ClassLoader userCodeClassLoader,
			MetricGroup consumerMetricGroup,
			boolean useMetrics) throws Exception {
		this.sourceContext = checkNotNull(sourceContext);
		this.checkpointLock = sourceContext.getCheckpointLock();
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);

		this.useMetrics = useMetrics;
		this.consumerMetricGroup = checkNotNull(consumerMetricGroup);
		this.legacyCurrentOffsetsMetricGroup = consumerMetricGroup.addGroup(LEGACY_CURRENT_OFFSETS_METRICS_GROUP);
		this.legacyCommittedOffsetsMetricGroup = consumerMetricGroup.addGroup(LEGACY_COMMITTED_OFFSETS_METRICS_GROUP);

		// figure out what we watermark mode we will be using
		this.watermarksPeriodic = watermarksPeriodic;
		this.watermarksPunctuated = watermarksPunctuated;

		if (watermarksPeriodic == null) {
			if (watermarksPunctuated == null) {
				// simple case, no watermarks involved
				timestampWatermarkMode = NO_TIMESTAMPS_WATERMARKS;
			} else {
				timestampWatermarkMode = PUNCTUATED_WATERMARKS;
			}
		} else {
			if (watermarksPunctuated == null) {
				timestampWatermarkMode = PERIODIC_WATERMARKS;
			} else {
				throw new IllegalArgumentException("Cannot have both periodic and punctuated watermarks");
			}
		}

		this.unassignedPartitionsQueue = new ClosableBlockingQueue<>();

		// initialize subscribed partition states with seed partitions
		this.subscribedPartitionStates = createPartitionStateHolders(
				seedPartitionsWithInitialOffsets,
				timestampWatermarkMode,
				watermarksPeriodic,
				watermarksPunctuated,
				userCodeClassLoader);

		// check that all seed partition states have a defined offset
		for (KafkaTopicPartitionState partitionState : subscribedPartitionStates) {
			if (!partitionState.isOffsetDefined()) {
				throw new IllegalArgumentException("The fetcher was assigned seed partitions with undefined initial offsets.");
			}
		}

		// all seed partitions are not assigned yet, so should be added to the unassigned partitions queue
		for (KafkaTopicPartitionState<KPH> partition : subscribedPartitionStates) {
			unassignedPartitionsQueue.add(partition);
		}

		// register metrics for the initial seed partitions
		if (useMetrics) {
			registerOffsetMetrics(consumerMetricGroup, subscribedPartitionStates);
		}

		// if we have periodic watermarks, kick off the interval scheduler
		if (timestampWatermarkMode == PERIODIC_WATERMARKS) {
			@SuppressWarnings("unchecked")
			PeriodicWatermarkEmitter periodicEmitter = new PeriodicWatermarkEmitter(
					subscribedPartitionStates,
					sourceContext,
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
		List<KafkaTopicPartitionState<KPH>> newPartitionStates = createPartitionStateHolders(
				newPartitions,
				KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET,
				timestampWatermarkMode,
				watermarksPeriodic,
				watermarksPunctuated,
				userCodeClassLoader);

		if (useMetrics) {
			registerOffsetMetrics(consumerMetricGroup, newPartitionStates);
		}

		for (KafkaTopicPartitionState<KPH> newPartitionState : newPartitionStates) {
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
	protected final List<KafkaTopicPartitionState<KPH>> subscribedPartitionStates() {
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
			.collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
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
		for (KafkaTopicPartitionState<KPH> partition : subscribedPartitionStates) {
			state.put(partition.getKafkaTopicPartition(), partition.getOffset());
		}
		return state;
	}

	// ------------------------------------------------------------------------
	//  emitting records
	// ------------------------------------------------------------------------

	/**
	 * Emits a record without attaching an existing timestamp to it.
	 *
	 * <p>Implementation Note: This method is kept brief to be JIT inlining friendly.
	 * That makes the fast path efficient, the extended paths are called as separate methods.
	 *
	 * @param record The record to emit
	 * @param partitionState The state of the Kafka partition from which the record was fetched
	 * @param offset The offset of the record
	 */
	protected void emitRecord(T record, KafkaTopicPartitionState<KPH> partitionState, long offset) throws Exception {

		if (record != null) {
			if (timestampWatermarkMode == NO_TIMESTAMPS_WATERMARKS) {
				// fast path logic, in case there are no watermarks

				// emit the record, using the checkpoint lock to guarantee
				// atomicity of record emission and offset state update
				synchronized (checkpointLock) {
					sourceContext.collect(record);
					partitionState.setOffset(offset);
				}
			} else if (timestampWatermarkMode == PERIODIC_WATERMARKS) {
				emitRecordWithTimestampAndPeriodicWatermark(record, partitionState, offset, Long.MIN_VALUE);
			} else {
				emitRecordWithTimestampAndPunctuatedWatermark(record, partitionState, offset, Long.MIN_VALUE);
			}
		} else {
			// if the record is null, simply just update the offset state for partition
			synchronized (checkpointLock) {
				partitionState.setOffset(offset);
			}
		}
	}

	/**
	 * Emits a record attaching a timestamp to it.
	 *
	 * <p>Implementation Note: This method is kept brief to be JIT inlining friendly.
	 * That makes the fast path efficient, the extended paths are called as separate methods.
	 *
	 * @param record The record to emit
	 * @param partitionState The state of the Kafka partition from which the record was fetched
	 * @param offset The offset of the record
	 */
	protected void emitRecordWithTimestamp(
			T record, KafkaTopicPartitionState<KPH> partitionState, long offset, long timestamp) throws Exception {

		if (record != null) {
			if (timestampWatermarkMode == NO_TIMESTAMPS_WATERMARKS) {
				// fast path logic, in case there are no watermarks generated in the fetcher

				// emit the record, using the checkpoint lock to guarantee
				// atomicity of record emission and offset state update
				synchronized (checkpointLock) {
					sourceContext.collectWithTimestamp(record, timestamp);
					partitionState.setOffset(offset);
				}
			} else if (timestampWatermarkMode == PERIODIC_WATERMARKS) {
				emitRecordWithTimestampAndPeriodicWatermark(record, partitionState, offset, timestamp);
			} else {
				emitRecordWithTimestampAndPunctuatedWatermark(record, partitionState, offset, timestamp);
			}
		} else {
			// if the record is null, simply just update the offset state for partition
			synchronized (checkpointLock) {
				partitionState.setOffset(offset);
			}
		}
	}

	/**
	 * Record emission, if a timestamp will be attached from an assigner that is
	 * also a periodic watermark generator.
	 */
	private void emitRecordWithTimestampAndPeriodicWatermark(
			T record, KafkaTopicPartitionState<KPH> partitionState, long offset, long kafkaEventTimestamp) {
		@SuppressWarnings("unchecked")
		final KafkaTopicPartitionStateWithPeriodicWatermarks<T, KPH> withWatermarksState =
				(KafkaTopicPartitionStateWithPeriodicWatermarks<T, KPH>) partitionState;

		// extract timestamp - this accesses/modifies the per-partition state inside the
		// watermark generator instance, so we need to lock the access on the
		// partition state. concurrent access can happen from the periodic emitter
		final long timestamp;
		//noinspection SynchronizationOnLocalVariableOrMethodParameter
		synchronized (withWatermarksState) {
			timestamp = withWatermarksState.getTimestampForRecord(record, kafkaEventTimestamp);
		}

		// emit the record with timestamp, using the usual checkpoint lock to guarantee
		// atomicity of record emission and offset state update
		synchronized (checkpointLock) {
			sourceContext.collectWithTimestamp(record, timestamp);
			partitionState.setOffset(offset);
		}
	}

	/**
	 * Record emission, if a timestamp will be attached from an assigner that is
	 * also a punctuated watermark generator.
	 */
	private void emitRecordWithTimestampAndPunctuatedWatermark(
			T record, KafkaTopicPartitionState<KPH> partitionState, long offset, long kafkaEventTimestamp) {
		@SuppressWarnings("unchecked")
		final KafkaTopicPartitionStateWithPunctuatedWatermarks<T, KPH> withWatermarksState =
				(KafkaTopicPartitionStateWithPunctuatedWatermarks<T, KPH>) partitionState;

		// only one thread ever works on accessing timestamps and watermarks
		// from the punctuated extractor
		final long timestamp = withWatermarksState.getTimestampForRecord(record, kafkaEventTimestamp);
		final Watermark newWatermark = withWatermarksState.checkAndGetNewWatermark(record, timestamp);

		// emit the record with timestamp, using the usual checkpoint lock to guarantee
		// atomicity of record emission and offset state update
		synchronized (checkpointLock) {
			sourceContext.collectWithTimestamp(record, timestamp);
			partitionState.setOffset(offset);
		}

		// if we also have a new per-partition watermark, check if that is also a
		// new cross-partition watermark
		if (newWatermark != null) {
			updateMinPunctuatedWatermark(newWatermark);
		}
	}

	/**
	 *Checks whether a new per-partition watermark is also a new cross-partition watermark.
	 */
	private void updateMinPunctuatedWatermark(Watermark nextWatermark) {
		if (nextWatermark.getTimestamp() > maxWatermarkSoFar) {
			long newMin = Long.MAX_VALUE;

			for (KafkaTopicPartitionState<?> state : subscribedPartitionStates) {
				@SuppressWarnings("unchecked")
				final KafkaTopicPartitionStateWithPunctuatedWatermarks<T, KPH> withWatermarksState =
						(KafkaTopicPartitionStateWithPunctuatedWatermarks<T, KPH>) state;

				newMin = Math.min(newMin, withWatermarksState.getCurrentPartitionWatermark());
			}

			// double-check locking pattern
			if (newMin > maxWatermarkSoFar) {
				synchronized (checkpointLock) {
					if (newMin > maxWatermarkSoFar) {
						maxWatermarkSoFar = newMin;
						sourceContext.emitWatermark(new Watermark(newMin));
					}
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Utility method that takes the topic partitions and creates the topic partition state
	 * holders, depending on the timestamp / watermark mode.
	 */
	private List<KafkaTopicPartitionState<KPH>> createPartitionStateHolders(
			Map<KafkaTopicPartition, Long> partitionsToInitialOffsets,
			int timestampWatermarkMode,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {

		List<KafkaTopicPartitionState<KPH>> partitionStates = new LinkedList<>();

		switch (timestampWatermarkMode) {
			case NO_TIMESTAMPS_WATERMARKS: {
				for (Map.Entry<KafkaTopicPartition, Long> partitionEntry : partitionsToInitialOffsets.entrySet()) {
					// create the kafka version specific partition handle
					KPH kafkaHandle = createKafkaPartitionHandle(partitionEntry.getKey());

					KafkaTopicPartitionState<KPH> partitionState =
							new KafkaTopicPartitionState<>(partitionEntry.getKey(), kafkaHandle);
					partitionState.setOffset(partitionEntry.getValue());

					partitionStates.add(partitionState);
				}

				return partitionStates;
			}

			case PERIODIC_WATERMARKS: {
				for (Map.Entry<KafkaTopicPartition, Long> partitionEntry : partitionsToInitialOffsets.entrySet()) {
					KPH kafkaHandle = createKafkaPartitionHandle(partitionEntry.getKey());

					AssignerWithPeriodicWatermarks<T> assignerInstance =
							watermarksPeriodic.deserializeValue(userCodeClassLoader);

					KafkaTopicPartitionStateWithPeriodicWatermarks<T, KPH> partitionState =
							new KafkaTopicPartitionStateWithPeriodicWatermarks<>(
									partitionEntry.getKey(),
									kafkaHandle,
									assignerInstance);

					partitionState.setOffset(partitionEntry.getValue());

					partitionStates.add(partitionState);
				}

				return partitionStates;
			}

			case PUNCTUATED_WATERMARKS: {
				for (Map.Entry<KafkaTopicPartition, Long> partitionEntry : partitionsToInitialOffsets.entrySet()) {
					KPH kafkaHandle = createKafkaPartitionHandle(partitionEntry.getKey());

					AssignerWithPunctuatedWatermarks<T> assignerInstance =
							watermarksPunctuated.deserializeValue(userCodeClassLoader);

					KafkaTopicPartitionStateWithPunctuatedWatermarks<T, KPH> partitionState =
							new KafkaTopicPartitionStateWithPunctuatedWatermarks<>(
									partitionEntry.getKey(),
									kafkaHandle,
									assignerInstance);

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
	 * Shortcut variant of {@link #createPartitionStateHolders(Map, int, SerializedValue, SerializedValue, ClassLoader)}
	 * that uses the same offset for all partitions when creating their state holders.
	 */
	private List<KafkaTopicPartitionState<KPH>> createPartitionStateHolders(
		List<KafkaTopicPartition> partitions,
		long initialOffset,
		int timestampWatermarkMode,
		SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
		SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
		ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {

		Map<KafkaTopicPartition, Long> partitionsToInitialOffset = new HashMap<>(partitions.size());
		for (KafkaTopicPartition partition : partitions) {
			partitionsToInitialOffset.put(partition, initialOffset);
		}

		return createPartitionStateHolders(
				partitionsToInitialOffset,
				timestampWatermarkMode,
				watermarksPeriodic,
				watermarksPunctuated,
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
			List<KafkaTopicPartitionState<KPH>> partitionOffsetStates) {

		for (KafkaTopicPartitionState<KPH> ktp : partitionOffsetStates) {
			MetricGroup topicPartitionGroup = consumerMetricGroup
				.addGroup(OFFSETS_BY_TOPIC_METRICS_GROUP, ktp.getTopic())
				.addGroup(OFFSETS_BY_PARTITION_METRICS_GROUP, Integer.toString(ktp.getPartition()));

			topicPartitionGroup.gauge(CURRENT_OFFSETS_METRICS_GAUGE, new OffsetGauge(ktp, OffsetGaugeType.CURRENT_OFFSET));
			topicPartitionGroup.gauge(COMMITTED_OFFSETS_METRICS_GAUGE, new OffsetGauge(ktp, OffsetGaugeType.COMMITTED_OFFSET));

			legacyCurrentOffsetsMetricGroup.gauge(getLegacyOffsetsMetricsGaugeName(ktp), new OffsetGauge(ktp, OffsetGaugeType.CURRENT_OFFSET));
			legacyCommittedOffsetsMetricGroup.gauge(getLegacyOffsetsMetricsGaugeName(ktp), new OffsetGauge(ktp, OffsetGaugeType.COMMITTED_OFFSET));
		}
	}

	private static String getLegacyOffsetsMetricsGaugeName(KafkaTopicPartitionState<?> ktp) {
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

		private final KafkaTopicPartitionState<?> ktp;
		private final OffsetGaugeType gaugeType;

		OffsetGauge(KafkaTopicPartitionState<?> ktp, OffsetGaugeType gaugeType) {
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
	private static class PeriodicWatermarkEmitter<KPH> implements ProcessingTimeCallback {

		private final List<KafkaTopicPartitionState<KPH>> allPartitions;

		private final SourceContext<?> emitter;

		private final ProcessingTimeService timerService;

		private final long interval;

		private long lastWatermarkTimestamp;

		//-------------------------------------------------

		PeriodicWatermarkEmitter(
				List<KafkaTopicPartitionState<KPH>> allPartitions,
				SourceContext<?> emitter,
				ProcessingTimeService timerService,
				long autoWatermarkInterval) {
			this.allPartitions = checkNotNull(allPartitions);
			this.emitter = checkNotNull(emitter);
			this.timerService = checkNotNull(timerService);
			this.interval = autoWatermarkInterval;
			this.lastWatermarkTimestamp = Long.MIN_VALUE;
		}

		//-------------------------------------------------

		public void start() {
			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
		}

		@Override
		public void onProcessingTime(long timestamp) throws Exception {

			long minAcrossAll = Long.MAX_VALUE;
			boolean isEffectiveMinAggregation = false;
			for (KafkaTopicPartitionState<?> state : allPartitions) {

				// we access the current watermark for the periodic assigners under the state
				// lock, to prevent concurrent modification to any internal variables
				final long curr;
				//noinspection SynchronizationOnLocalVariableOrMethodParameter
				synchronized (state) {
					curr = ((KafkaTopicPartitionStateWithPeriodicWatermarks<?, ?>) state).getCurrentWatermarkTimestamp();
				}

				minAcrossAll = Math.min(minAcrossAll, curr);
				isEffectiveMinAggregation = true;
			}

			// emit next watermark, if there is one
			if (isEffectiveMinAggregation && minAcrossAll > lastWatermarkTimestamp) {
				lastWatermarkTimestamp = minAcrossAll;
				emitter.emitWatermark(new Watermark(minAcrossAll));
			}

			// schedule the next watermark
			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
		}
	}
}
