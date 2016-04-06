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

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
	
	private static final int NO_TIMESTAMPS_WATERMARKS = 0;
	private static final int PERIODIC_WATERMARKS = 1;
	private static final int PUNCTUATED_WATERMARKS = 2;
	
	// ------------------------------------------------------------------------
	
	/** The source context to emit records and watermarks to */
	private final SourceContext<T> sourceContext;

	/** The lock that guarantees that record emission and state updates are atomic,
	 * from the view of taking a checkpoint */
	private final Object checkpointLock;

	/** All partitions (and their state) that this fetcher is subscribed to */
	private final KafkaTopicPartitionState<KPH>[] allPartitions;

	/** The mode describing whether the fetcher also generates timestamps and watermarks */
	private final int timestampWatermarkMode;
	
	/** Only relevant for punctuated watermarks: The current cross partition watermark */
	private volatile long maxWatermarkSoFar = Long.MIN_VALUE;

	// ------------------------------------------------------------------------
	
	protected AbstractFetcher(
			SourceContext<T> sourceContext,
			List<KafkaTopicPartition> assignedPartitions,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			StreamingRuntimeContext runtimeContext) throws Exception
	{
		this.sourceContext = checkNotNull(sourceContext);
		this.checkpointLock = sourceContext.getCheckpointLock();
		
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
		this.allPartitions = initializePartitions(
				assignedPartitions,
				timestampWatermarkMode,
				watermarksPeriodic, watermarksPunctuated,
				runtimeContext.getUserCodeClassLoader());
		
		// if we have periodic watermarks, kick off the interval scheduler
		if (timestampWatermarkMode == PERIODIC_WATERMARKS) {
			KafkaTopicPartitionStateWithPeriodicWatermarks<?, ?>[] parts = 
					(KafkaTopicPartitionStateWithPeriodicWatermarks<?, ?>[]) allPartitions;
			
			PeriodicWatermarkEmitter periodicEmitter = 
					new PeriodicWatermarkEmitter(parts, sourceContext, runtimeContext);
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
	protected final KafkaTopicPartitionState<KPH>[] subscribedPartitions() {
		return allPartitions;
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
	 * older Kafka versions).
	 * 
	 * @param offsets The offsets to commit to Kafka.
	 * @throws Exception This method forwards exceptions.
	 */
	public abstract void commitSpecificOffsetsToKafka(Map<KafkaTopicPartition, Long> offsets) throws Exception;
	
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

		HashMap<KafkaTopicPartition, Long> state = new HashMap<>(allPartitions.length);
		for (KafkaTopicPartitionState<?> partition : subscribedPartitions()) {
			if (partition.isOffsetDefined()) {
				state.put(partition.getKafkaTopicPartition(), partition.getOffset());
			}
		}
		return state;
	}

	/**
	 * Restores the partition offsets.
	 * 
	 * @param snapshotState The offsets for the partitions 
	 */
	public void restoreOffsets(HashMap<KafkaTopicPartition, Long> snapshotState) {
		for (KafkaTopicPartitionState<?> partition : allPartitions) {
			Long offset = snapshotState.get(partition.getKafkaTopicPartition());
			if (offset != null) {
				partition.setOffset(offset);
			}
		}
	}
	
	// ------------------------------------------------------------------------
	//  emitting records
	// ------------------------------------------------------------------------

	/**
	 * 
	 * <p>Implementation Note: This method is kept brief to be JIT inlining friendly.
	 * That makes the fast path efficient, the extended paths are called as separate methods.
	 * 
	 * @param record The record to emit
	 * @param partitionState The state of the Kafka partition from which the record was fetched
	 * @param offset The offset from which the record was fetched
	 */
	protected final void emitRecord(T record, KafkaTopicPartitionState<KPH> partitionState, long offset) {
		if (timestampWatermarkMode == NO_TIMESTAMPS_WATERMARKS) {
			// fast path logic, in case there are no watermarks

			// emit the record, using the checkpoint lock to guarantee
			// atomicity of record emission and offset state update
			synchronized (checkpointLock) {
				sourceContext.collect(record);
				partitionState.setOffset(offset);
			}
		}
		else if (timestampWatermarkMode == PERIODIC_WATERMARKS) {
			emitRecordWithTimestampAndPeriodicWatermark(record, partitionState, offset);
		}
		else {
			emitRecordWithTimestampAndPunctuatedWatermark(record, partitionState, offset);
		}
	}

	/**
	 * Record emission, if a timestamp will be attached from an assigner that is
	 * also a periodic watermark generator.
	 */
	private void emitRecordWithTimestampAndPeriodicWatermark(
			T record, KafkaTopicPartitionState<KPH> partitionState, long offset)
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
			timestamp = withWatermarksState.getTimestampForRecord(record);
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
			T record, KafkaTopicPartitionState<KPH> partitionState, long offset)
	{
		@SuppressWarnings("unchecked")
		final KafkaTopicPartitionStateWithPunctuatedWatermarks<T, KPH> withWatermarksState =
				(KafkaTopicPartitionStateWithPunctuatedWatermarks<T, KPH>) partitionState;

		// only one thread ever works on accessing timestamps and watermarks
		// from the punctuated extractor
		final long timestamp = withWatermarksState.getTimestampForRecord(record);
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
			
			for (KafkaTopicPartitionState<?> state : allPartitions) {
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
	private KafkaTopicPartitionState<KPH>[] initializePartitions(
			List<KafkaTopicPartition> assignedPartitions,
			int timestampWatermarkMode,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			ClassLoader userCodeClassLoader)
		throws IOException, ClassNotFoundException
	{
		@SuppressWarnings("unchecked")
		KafkaTopicPartitionState<KPH>[] partitions =
				(KafkaTopicPartitionState<KPH>[]) new KafkaTopicPartitionState<?>[assignedPartitions.size()];

		int pos = 0;
		for (KafkaTopicPartition partition : assignedPartitions) {
			// create the kafka version specific partition handle
			KPH kafkaHandle = createKafkaPartitionHandle(partition);
			
			// create the partition state
			KafkaTopicPartitionState<KPH> partitionState;
			switch (timestampWatermarkMode) {
				case NO_TIMESTAMPS_WATERMARKS:
					partitionState = new KafkaTopicPartitionState<>(partition, kafkaHandle);
					break;
				case PERIODIC_WATERMARKS: {
					AssignerWithPeriodicWatermarks<T> assignerInstance =
							watermarksPeriodic.deserializeValue(userCodeClassLoader);
					partitionState = new KafkaTopicPartitionStateWithPeriodicWatermarks<>(
							partition, kafkaHandle, assignerInstance);
					break;
				}
					
				case PUNCTUATED_WATERMARKS: {
					AssignerWithPunctuatedWatermarks<T> assignerInstance =
							watermarksPunctuated.deserializeValue(userCodeClassLoader);
					partitionState = new KafkaTopicPartitionStateWithPunctuatedWatermarks<>(
							partition, kafkaHandle, assignerInstance);
					break;
				}
				default:
					// cannot happen, add this as a guard for the future
					throw new RuntimeException();
			}

			partitions[pos++] = partitionState;
		}
		
		return partitions;
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * The periodic watermark emitter. In its given interval, it checks all partitions for
	 * the current event time watermark, and possibly emits the next watermark.
	 */
	private static class PeriodicWatermarkEmitter implements Triggerable {

		private final KafkaTopicPartitionStateWithPeriodicWatermarks<?, ?>[] allPartitions;
		
		private final SourceContext<?> emitter;
		
		private final StreamingRuntimeContext triggerContext;

		private final long interval;
		
		private long lastWatermarkTimestamp;
		
		//-------------------------------------------------

		PeriodicWatermarkEmitter(
				KafkaTopicPartitionStateWithPeriodicWatermarks<?, ?>[] allPartitions,
				SourceContext<?> emitter,
				StreamingRuntimeContext runtimeContext)
		{
			this.allPartitions = checkNotNull(allPartitions);
			this.emitter = checkNotNull(emitter);
			this.triggerContext = checkNotNull(runtimeContext);
			this.interval = runtimeContext.getExecutionConfig().getAutoWatermarkInterval();
			this.lastWatermarkTimestamp = Long.MIN_VALUE;
		}

		//-------------------------------------------------
		
		public void start() {
			triggerContext.registerTimer(System.currentTimeMillis() + interval, this);
		}
		
		@Override
		public void trigger(long timestamp) throws Exception {
			// sanity check
			assert Thread.holdsLock(emitter.getCheckpointLock());
			
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
			triggerContext.registerTimer(System.currentTimeMillis() + interval, this);
		}
	}
}
