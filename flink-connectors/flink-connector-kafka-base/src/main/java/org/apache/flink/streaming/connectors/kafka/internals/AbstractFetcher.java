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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
public abstract class AbstractFetcher<T, KPH> {
	
	protected static final int NO_TIMESTAMPS_WATERMARKS = 0;
	protected static final int PERIODIC_WATERMARKS = 1;
	protected static final int PUNCTUATED_WATERMARKS = 2;
	
	// ------------------------------------------------------------------------
	
	/** The source context to emit records and watermarks to */
	protected final SourceContext<T> sourceContext;

	/** The lock that guarantees that record emission and state updates are atomic,
	 * from the view of taking a checkpoint */
	protected final Object checkpointLock;

	/** All partitions (and their state) that this fetcher is subscribed to */
	private final KafkaTopicPartitionState<KPH>[] subscribedPartitionStates;

	/** The mode describing whether the fetcher also generates timestamps and watermarks */
	protected final int timestampWatermarkMode;

	/** Flag whether to register metrics for the fetcher */
	protected final boolean useMetrics;

	/** Only relevant for punctuated watermarks: The current cross partition watermark */
	private volatile long maxWatermarkSoFar = Long.MIN_VALUE;

	// ------------------------------------------------------------------------
	
	protected AbstractFetcher(
			SourceContext<T> sourceContext,
			Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			ProcessingTimeService processingTimeProvider,
			long autoWatermarkInterval,
			ClassLoader userCodeClassLoader,
			boolean useMetrics) throws Exception
	{
		this.sourceContext = checkNotNull(sourceContext);
		this.checkpointLock = sourceContext.getCheckpointLock();
		this.useMetrics = useMetrics;
		
		// figure out what we watermark mode we will be using
		
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

		// create our partition state according to the timestamp/watermark mode 
		this.subscribedPartitionStates = initializeSubscribedPartitionStates(
				assignedPartitionsWithInitialOffsets,
				timestampWatermarkMode,
				watermarksPeriodic, watermarksPunctuated,
				userCodeClassLoader);

		// check that all partition states have a defined offset
		for (KafkaTopicPartitionState partitionState : subscribedPartitionStates) {
			if (!partitionState.isOffsetDefined()) {
				throw new IllegalArgumentException("The fetcher was assigned partitions with undefined initial offsets.");
			}
		}
		
		// if we have periodic watermarks, kick off the interval scheduler
		if (timestampWatermarkMode == PERIODIC_WATERMARKS) {
			KafkaTopicPartitionStateWithPeriodicWatermarks<?, ?>[] parts = 
					(KafkaTopicPartitionStateWithPeriodicWatermarks<?, ?>[]) subscribedPartitionStates;
			
			PeriodicWatermarkEmitter periodicEmitter = 
					new PeriodicWatermarkEmitter(parts, sourceContext, processingTimeProvider, autoWatermarkInterval);
			periodicEmitter.start();
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
	protected final KafkaTopicPartitionState<KPH>[] subscribedPartitionStates() {
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
	 * Creates the Kafka version specific representation of the given
	 * topic partition.
	 * 
	 * @param partition The Flink representation of the Kafka topic partition.
	 * @return The specific Kafka representation of the Kafka topic partition.
	 */
	public abstract KPH createKafkaPartitionHandle(KafkaTopicPartition partition);

	/**
	 * Commits the given partition offsets to the Kafka brokers (or to ZooKeeper for
	 * older Kafka versions). This method is only ever called when the offset commit mode of
	 * the consumer is {@link OffsetCommitMode#ON_CHECKPOINTS}.
	 *
	 * The given offsets are the internal checkpointed offsets, representing
	 * the last processed record of each partition. Version-specific implementations of this method
	 * need to hold the contract that the given offsets must be incremented by 1 before
	 * committing them, so that committed offsets to Kafka represent "the next record to process".
	 *
	 * @param offsets The offsets to commit to Kafka (implementations must increment offsets by 1 before committing).
	 * @throws Exception This method forwards exceptions.
	 */
	public abstract void commitInternalOffsetsToKafka(Map<KafkaTopicPartition, Long> offsets) throws Exception;
	
	// ------------------------------------------------------------------------
	//  snapshot and restore the state
	// ------------------------------------------------------------------------

	/**
	 * Takes a snapshot of the partition offsets.
	 * 
	 * <p>Important: This method mus be called under the checkpoint lock.
	 * 
	 * @return A map from partition to current offset.
	 */
	public HashMap<KafkaTopicPartition, Long> snapshotCurrentState() {
		// this method assumes that the checkpoint lock is held
		assert Thread.holdsLock(checkpointLock);

		HashMap<KafkaTopicPartition, Long> state = new HashMap<>(subscribedPartitionStates.length);
		for (KafkaTopicPartitionState<?> partition : subscribedPartitionStates()) {
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
	protected void emitRecordWithTimestampAndPeriodicWatermark(
			T record, KafkaTopicPartitionState<KPH> partitionState, long offset, long kafkaEventTimestamp)
	{
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
	protected void emitRecordWithTimestampAndPunctuatedWatermark(
			T record, KafkaTopicPartitionState<KPH> partitionState, long offset, long kafkaEventTimestamp)
	{
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
	 * holders. If a watermark generator per partition exists, this will also initialize those.
	 */
	private KafkaTopicPartitionState<KPH>[] initializeSubscribedPartitionStates(
			Map<KafkaTopicPartition, Long> assignedPartitionsToInitialOffsets,
			int timestampWatermarkMode,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			ClassLoader userCodeClassLoader)
		throws IOException, ClassNotFoundException
	{
		switch (timestampWatermarkMode) {
			
			case NO_TIMESTAMPS_WATERMARKS: {
				@SuppressWarnings("unchecked")
				KafkaTopicPartitionState<KPH>[] partitions =
						(KafkaTopicPartitionState<KPH>[]) new KafkaTopicPartitionState<?>[assignedPartitionsToInitialOffsets.size()];

				int pos = 0;
				for (Map.Entry<KafkaTopicPartition, Long> partition : assignedPartitionsToInitialOffsets.entrySet()) {
					// create the kafka version specific partition handle
					KPH kafkaHandle = createKafkaPartitionHandle(partition.getKey());
					partitions[pos] = new KafkaTopicPartitionState<>(partition.getKey(), kafkaHandle);
					partitions[pos].setOffset(partition.getValue());

					pos++;
				}

				return partitions;
			}

			case PERIODIC_WATERMARKS: {
				@SuppressWarnings("unchecked")
				KafkaTopicPartitionStateWithPeriodicWatermarks<T, KPH>[] partitions =
						(KafkaTopicPartitionStateWithPeriodicWatermarks<T, KPH>[])
								new KafkaTopicPartitionStateWithPeriodicWatermarks<?, ?>[assignedPartitionsToInitialOffsets.size()];

				int pos = 0;
				for (Map.Entry<KafkaTopicPartition, Long> partition : assignedPartitionsToInitialOffsets.entrySet()) {
					KPH kafkaHandle = createKafkaPartitionHandle(partition.getKey());

					AssignerWithPeriodicWatermarks<T> assignerInstance =
							watermarksPeriodic.deserializeValue(userCodeClassLoader);
					
					partitions[pos] = new KafkaTopicPartitionStateWithPeriodicWatermarks<>(
							partition.getKey(), kafkaHandle, assignerInstance);
					partitions[pos].setOffset(partition.getValue());

					pos++;
				}

				return partitions;
			}

			case PUNCTUATED_WATERMARKS: {
				@SuppressWarnings("unchecked")
				KafkaTopicPartitionStateWithPunctuatedWatermarks<T, KPH>[] partitions =
						(KafkaTopicPartitionStateWithPunctuatedWatermarks<T, KPH>[])
								new KafkaTopicPartitionStateWithPunctuatedWatermarks<?, ?>[assignedPartitionsToInitialOffsets.size()];

				int pos = 0;
				for (Map.Entry<KafkaTopicPartition, Long> partition : assignedPartitionsToInitialOffsets.entrySet()) {
					KPH kafkaHandle = createKafkaPartitionHandle(partition.getKey());

					AssignerWithPunctuatedWatermarks<T> assignerInstance =
							watermarksPunctuated.deserializeValue(userCodeClassLoader);

					partitions[pos] = new KafkaTopicPartitionStateWithPunctuatedWatermarks<>(
							partition.getKey(), kafkaHandle, assignerInstance);
					partitions[pos].setOffset(partition.getValue());

					pos++;
				}

				return partitions;
			}
			default:
				// cannot happen, add this as a guard for the future
				throw new RuntimeException();
		}
	}

	// ------------------------- Metrics ----------------------------------

	/**
	 * Add current and committed offsets to metric group
	 *
	 * @param metricGroup The metric group to use
	 */
	protected void addOffsetStateGauge(MetricGroup metricGroup) {
		// add current offsets to gage
		MetricGroup currentOffsets = metricGroup.addGroup("current-offsets");
		MetricGroup committedOffsets = metricGroup.addGroup("committed-offsets");
		for (KafkaTopicPartitionState<?> ktp: subscribedPartitionStates()) {
			currentOffsets.gauge(ktp.getTopic() + "-" + ktp.getPartition(), new OffsetGauge(ktp, OffsetGaugeType.CURRENT_OFFSET));
			committedOffsets.gauge(ktp.getTopic() + "-" + ktp.getPartition(), new OffsetGauge(ktp, OffsetGaugeType.COMMITTED_OFFSET));
		}
	}

	/**
	 * Gauge types
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

		public OffsetGauge(KafkaTopicPartitionState<?> ktp, OffsetGaugeType gaugeType) {
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
	private static class PeriodicWatermarkEmitter implements ProcessingTimeCallback {

		private final KafkaTopicPartitionStateWithPeriodicWatermarks<?, ?>[] allPartitions;
		
		private final SourceContext<?> emitter;
		
		private final ProcessingTimeService timerService;

		private final long interval;
		
		private long lastWatermarkTimestamp;
		
		//-------------------------------------------------

		PeriodicWatermarkEmitter(
				KafkaTopicPartitionStateWithPeriodicWatermarks<?, ?>[] allPartitions,
				SourceContext<?> emitter,
				ProcessingTimeService timerService,
				long autoWatermarkInterval)
		{
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
			for (KafkaTopicPartitionStateWithPeriodicWatermarks<?, ?> state : allPartitions) {
				
				// we access the current watermark for the periodic assigners under the state
				// lock, to prevent concurrent modification to any internal variables
				final long curr;
				//noinspection SynchronizationOnLocalVariableOrMethodParameter
				synchronized (state) {
					curr = state.getCurrentWatermarkTimestamp();
				}
				
				minAcrossAll = Math.min(minAcrossAll, curr);
			}
			
			// emit next watermark, if there is one
			if (minAcrossAll > lastWatermarkTimestamp) {
				lastWatermarkTimestamp = minAcrossAll;
				emitter.emitWatermark(new Watermark(minAcrossAll));
			}
			
			// schedule the next watermark
			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
		}
	}
}
