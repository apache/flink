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

import org.apache.commons.collections.map.LinkedMap;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaPartitionState;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.streaming.connectors.kafka.util.KafkaUtils.checkArgument;

public abstract class FlinkKafkaConsumerBase<T> extends RichParallelSourceFunction<T>
		implements CheckpointListener, CheckpointedAsynchronously<HashMap<KafkaTopicPartition, Long>>, ResultTypeQueryable<T>, Triggerable {

	// ------------------------------------------------------------------------

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaConsumerBase.class);

	private static final long serialVersionUID = -6272159445203409112L;

	/** Magic number to define an unset offset. Negative offsets are not used by Kafka (invalid),
	 * and we pick a number that is probably (hopefully) not used by Kafka as a magic number for anything else. */
	public static final long OFFSET_NOT_SET = -915623761776L;

	/** The maximum number of pending non-committed checkpoints to track, to avoid memory leaks */
	public static final int MAX_NUM_PENDING_CHECKPOINTS = 100;


	/** The schema to convert between Kafka#s byte messages, and Flink's objects */
	protected final KeyedDeserializationSchema<T> deserializer;

	// ------  Runtime State  -------

	/** Data for pending but uncommitted checkpoints */
	protected final LinkedMap pendingCheckpoints = new LinkedMap();

	/**
	 * Information about the partitions being read by the local consumer. This contains:
	 * offsets of the last returned elements, and if a timestamp assigner is used, it
	 * also contains the maximum seen timestamp in the partition and if the partition
	 * still receives elements or it is inactive.
	 */
	protected transient HashMap<KafkaTopicPartition, KafkaPartitionState> partitionState;

	/** The offsets to restore to, if the consumer restores state from a checkpoint */
	protected transient HashMap<KafkaTopicPartition, Long> restoreToOffset;

	/** Flag indicating whether the consumer is still running **/
	protected volatile boolean running = true;

	// ------------------------------------------------------------------------
	//							WATERMARK EMISSION
	// ------------------------------------------------------------------------

	/**
	 * The user-specified methods to extract the timestamps from the records in Kafka, and
	 * to decide when to emit watermarks.
	 */
	private AssignerWithPunctuatedWatermarks<T> punctuatedWatermarkAssigner;

	/**
	 * The user-specified methods to extract the timestamps from the records in Kafka, and
	 * to decide when to emit watermarks.
	 */
	private AssignerWithPeriodicWatermarks<T> periodicWatermarkAssigner;

	private StreamingRuntimeContext runtime = null;

	private SourceContext<T> srcContext = null;

	/**
	 * The interval between consecutive periodic watermark emissions,
	 * as configured via the {@link ExecutionConfig#getAutoWatermarkInterval()}.
	 */
	private long watermarkInterval = -1;

	/** The last emitted watermark. */
	private long lastEmittedWatermark = Long.MIN_VALUE;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new Flink Kafka Consumer, using the given type of fetcher and offset handler.
	 *
	 * <p>To determine which kink of fetcher and offset handler to use, please refer to the docs
	 * at the beginning of this class.</p>
	 *
	 * @param deserializer
	 *           The deserializer to turn raw byte messages into Java/Scala objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkKafkaConsumerBase(KeyedDeserializationSchema<T> deserializer, Properties props) {
		this.deserializer = requireNonNull(deserializer, "valueDeserializer");
	}

	/**
	 * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks in a punctuated manner. Bare in mind
	 * that the source can either have an {@link AssignerWithPunctuatedWatermarks} or an
	 * {@link AssignerWithPeriodicWatermarks}, not both.
	 */
	public FlinkKafkaConsumerBase<T> setPunctuatedWatermarkEmitter(AssignerWithPunctuatedWatermarks<T> assigner) {
		checkEmitterDuringInit();
		this.punctuatedWatermarkAssigner = assigner;
		return this;
	}

	/**
	 * Specifies an {@link AssignerWithPeriodicWatermarks} to emit watermarks periodically. Bare in mind that the
	 * source can either have an {@link AssignerWithPunctuatedWatermarks} or an
	 * {@link AssignerWithPeriodicWatermarks}, not both.
	 */
	public FlinkKafkaConsumerBase<T> setPeriodicWatermarkEmitter(AssignerWithPeriodicWatermarks<T> assigner) {
		checkEmitterDuringInit();
		this.periodicWatermarkAssigner = assigner;
		return this;
	}

	/**
	 * Processes the element after having been read from Kafka and deserialized, and updates the
	 * last read offset for the specifies partition. These two actions should be performed in
	 * an atomic way in order to guarantee exactly once semantics.
	 * @param sourceContext
	 *           The context the task operates in.
	 * @param partDescriptor
	 *            A descriptor containing the topic and the id of the partition.
	 * @param value
	 *           The element to process.
	 * @param offset
	 *           The offset of the element in the partition.
	 * */
	public void processElement(SourceContext<T> sourceContext, KafkaTopicPartition partDescriptor, T value, long offset) {
		if (punctuatedWatermarkAssigner == null && periodicWatermarkAssigner == null) {
			// the case where no watermark emitter is specified.
			sourceContext.collect(value);
		} else {

			if (srcContext == null) {
				srcContext = sourceContext;
			}

			long extractedTimestamp = extractTimestampAndEmitElement(partDescriptor, value);

			// depending on the specified watermark emitter, either send a punctuated watermark,
			// or set the timer for the first periodic watermark. In the periodic case, we set the timer
			// only for the first watermark, as it is the trigger() that will set the subsequent ones.

			if (punctuatedWatermarkAssigner != null) {
				final Watermark nextWatermark = punctuatedWatermarkAssigner
					.checkAndGetNextWatermark(value, extractedTimestamp);
				if (nextWatermark != null) {
					emitWatermarkIfMarkingProgress(sourceContext);
				}
			} else if(periodicWatermarkAssigner != null && runtime == null) {
				runtime = (StreamingRuntimeContext) getRuntimeContext();
				watermarkInterval = runtime.getExecutionConfig().getAutoWatermarkInterval();
				if (watermarkInterval > 0) {
					runtime.registerTimer(System.currentTimeMillis() + watermarkInterval, this);
				}
			}
		}
		updateOffsetForPartition(partDescriptor, offset);
	}

	/**
	 * Extract the timestamp from the element based on the user-specified extractor,
	 * emit the element with the new timestamp, and update the partition monitoring info (if necessary).
	 * In more detail, upon reception of an element with a timestamp greater than the greatest timestamp
	 * seen so far in that partition, this method updates the maximum timestamp seen for that partition,
	 * and marks the partition as {@code active}, i.e. it still receives fresh data.
	 * @param partDescriptor the partition the new element belongs to.
	 * @param value the element to be forwarded.
	 * @return the timestamp of the new element.
	 */
	private long extractTimestampAndEmitElement(KafkaTopicPartition partDescriptor, T value) {
		long extractedTimestamp = getTimestampAssigner().extractTimestamp(value, Long.MIN_VALUE);
		srcContext.collectWithTimestamp(value, extractedTimestamp);
		updateMaximumTimestampForPartition(partDescriptor, extractedTimestamp);
		return extractedTimestamp;
	}

	/**
	 * Upon reception of an element with a timestamp greater than the greatest timestamp seen so far in the partition,
	 * this method updates the maximum timestamp seen for that partition to {@code timestamp}, and marks the partition
	 * as {@code active}, i.e. it still receives fresh data. If the partition is not known to the system, then a new
	 * {@link KafkaPartitionState} is created and is associated to the new partition for future monitoring.
	 * @param partDescriptor
	 *            A descriptor containing the topic and the id of the partition.
	 * @param timestamp
	 *           The timestamp to set the minimum to, if smaller than the already existing one.
	 * @return {@code true} if the minimum was updated successfully to {@code timestamp}, {@code false}
	 *           if the previous value is smaller than the provided timestamp
	 * */
	private boolean updateMaximumTimestampForPartition(KafkaTopicPartition partDescriptor, long timestamp) {
		KafkaPartitionState info = getOrInitializeInfo(partDescriptor);

		if(timestamp > info.getMaxTimestamp()) {

			// the flag is set to false as soon as the current partition's max timestamp is sent as a watermark.
			// if then, and for that partition, only late elements arrive, then the max timestamp will stay the
			// same, and it will keep the overall system from progressing.
			// To avoid this, we only mark a partition as active on non-late elements.

			info.setActive(true);
			info.setMaxTimestamp(timestamp);
			return  true;
		}
		return false;
	}

	/**
	 * Updates the last read offset for the partition specified by the {@code partDescriptor} to {@code offset}.
	 * If it is the first time we see the partition, then a new {@link KafkaPartitionState} is created to monitor
	 * this specific partition.
	 * @param partDescriptor the partition whose info to update.
	 * @param offset the last read offset of the partition.
	 */
	public void updateOffsetForPartition(KafkaTopicPartition partDescriptor, long offset) {
		KafkaPartitionState info = getOrInitializeInfo(partDescriptor);
		info.setOffset(offset);
	}

	@Override
	public void trigger(long timestamp) throws Exception {
		if(this.srcContext == null) {
			// if the trigger is called before any elements, then we
			// just set the next timer to fire when it should and we
			// ignore the triggering as this would produce no results.
			setNextWatermarkTimer();
			return;
		}

		// this is valid because this method is only called when watermarks
		// are set to be emitted periodically.
		final Watermark nextWatermark = periodicWatermarkAssigner.getCurrentWatermark();
		if(nextWatermark != null) {
			emitWatermarkIfMarkingProgress(srcContext);
		}
		setNextWatermarkTimer();
	}

	/**
	 * Emits a new watermark, with timestamp equal to the minimum across all the maximum timestamps
	 * seen per local partition (across all topics). The new watermark is emitted if and only if
	 * it signals progress in event-time, i.e. if its timestamp is greater than the timestamp of
	 * the last emitted watermark. In addition, this method marks as inactive the partition whose
	 * timestamp was emitted as watermark, i.e. the one with the minimum across the maximum timestamps
	 * of the local partitions. This is done to avoid not making progress because
	 * a partition stopped receiving data. The partition is going to be marked as {@code active}
	 * as soon as the <i>next non-late</i> element arrives.
	 *
	 * @return {@code true} if the Watermark was successfully emitted, {@code false} otherwise.
	 */
	private boolean emitWatermarkIfMarkingProgress(SourceFunction.SourceContext<T> sourceContext) {
		Tuple2<KafkaTopicPartition, Long> globalMinTs = getMinTimestampAcrossAllTopics();
		if(globalMinTs.f0 != null ) {
			synchronized (sourceContext.getCheckpointLock()) {
				long minTs = globalMinTs.f1;
				if(minTs > lastEmittedWatermark) {
					lastEmittedWatermark = minTs;
					Watermark toEmit = new Watermark(minTs);
					sourceContext.emitWatermark(toEmit);
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Kafka sources with timestamp extractors are expected to keep the maximum timestamp seen per
	 * partition they are reading from. This is to mark the per-partition event-time progress.
	 *
	 * This method iterates this list, and returns the minimum timestamp across these per-partition
	 * max timestamps, and across all topics. In addition to this information, it also returns the topic and
	 * the partition within the topic the timestamp belongs to.
	 */
	private Tuple2<KafkaTopicPartition, Long> getMinTimestampAcrossAllTopics() {
		Tuple2<KafkaTopicPartition, Long> minTimestamp = new Tuple2<>(null, Long.MAX_VALUE);
		for(Map.Entry<KafkaTopicPartition, KafkaPartitionState> entries: partitionState.entrySet()) {
			KafkaTopicPartition part = entries.getKey();
			KafkaPartitionState info = entries.getValue();

			if(partitionIsActive(part) && info.getMaxTimestamp() < minTimestamp.f1) {
				minTimestamp.f0 = part;
				minTimestamp.f1 = info.getMaxTimestamp();
			}
		}

		if(minTimestamp.f0 != null) {
			// it means that we have a winner and we have to set its flag to
			// inactive, until its next non-late element.
			KafkaTopicPartition partitionDescriptor = minTimestamp.f0;
			setActiveFlagForPartition(partitionDescriptor, false);
		}

		return minTimestamp;
	}

	/**
	 * Sets the {@code active} flag for a given partition of a topic to {@code isActive}.
	 * This flag signals if the partition is still receiving data and it is used to avoid the case
	 * where a partition stops receiving data, so its max seen timestamp does not advance, and it
	 * holds back the progress of the watermark for all partitions. Note that if the partition is
	 * not known to the system, then a new {@link KafkaPartitionState} is created and is associated
	 * to the new partition for future monitoring.
	 *
	 * @param partDescriptor
	 * 				A descriptor containing the topic and the id of the partition.
	 * @param isActive
	 * 				The value {@code true} or {@code false} to set the flag to.
	 */
	private void setActiveFlagForPartition(KafkaTopicPartition partDescriptor, boolean isActive) {
		KafkaPartitionState info = getOrInitializeInfo(partDescriptor);
		info.setActive(isActive);
	}

	/**
	 * Gets the statistics for a given partition specified by the {@code partition} argument.
	 * If it is the first time we see this partition, a new {@link KafkaPartitionState} data structure
	 * is initialized to monitor it from now on. This method never throws a {@link NullPointerException}.
	 * @param partition the partition to be fetched.
	 * @return the gathered statistics for that partition.
	 * */
	private KafkaPartitionState getOrInitializeInfo(KafkaTopicPartition partition) {
		KafkaPartitionState info = partitionState.get(partition);
		if(info == null) {
			info = new KafkaPartitionState(partition.getPartition(), FlinkKafkaConsumerBase.OFFSET_NOT_SET);
			partitionState.put(partition, info);
		}
		return info;
	}

	/**
	 * Checks if a partition of a topic is still active, i.e. if it still receives data.
	 * @param partDescriptor
	 *          A descriptor containing the topic and the id of the partition.
	 * */
	private boolean partitionIsActive(KafkaTopicPartition partDescriptor) {
		KafkaPartitionState info = partitionState.get(partDescriptor);
		if(info == null) {
			throw new RuntimeException("Unknown Partition: Topic=" + partDescriptor.getTopic() +
				" Partition=" + partDescriptor.getPartition());
		}
		return info.isActive();
	}

	private TimestampAssigner<T> getTimestampAssigner() {
		checkEmitterStateAfterInit();
		return periodicWatermarkAssigner != null ? periodicWatermarkAssigner : punctuatedWatermarkAssigner;
	}

	private void setNextWatermarkTimer() {
		long timeToNextWatermark = System.currentTimeMillis() + watermarkInterval;
		runtime.registerTimer(timeToNextWatermark, this);
	}

	private void checkEmitterDuringInit() {
		if(periodicWatermarkAssigner != null) {
			throw new RuntimeException("A periodic watermark emitter has already been provided.");
		} else if(punctuatedWatermarkAssigner != null) {
			throw new RuntimeException("A punctuated watermark emitter has already been provided.");
		}
	}

	private void checkEmitterStateAfterInit() {
		if(periodicWatermarkAssigner == null && punctuatedWatermarkAssigner == null) {
			throw new RuntimeException("The timestamp assigner has not been initialized.");
		} else if(periodicWatermarkAssigner != null && punctuatedWatermarkAssigner != null) {
			throw new RuntimeException("The source can either have an assigner with punctuated " +
				"watermarks or one with periodic watermarks, not both.");
		}
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and restore
	// ------------------------------------------------------------------------

	HashMap<KafkaTopicPartition, KafkaPartitionState> restoreInfoFromCheckpoint() {
		HashMap<KafkaTopicPartition, KafkaPartitionState> partInfo = new HashMap<>(restoreToOffset.size());
		for(Map.Entry<KafkaTopicPartition, Long> offsets: restoreToOffset.entrySet()) {
			KafkaTopicPartition key = offsets.getKey();
			partInfo.put(key, new KafkaPartitionState(key.getPartition(), offsets.getValue()));
		}
		return partInfo;
	}

	@Override
	public HashMap<KafkaTopicPartition, Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		if (partitionState == null) {
			LOG.debug("snapshotState() requested on not yet opened source; returning null.");
			return null;
		}
		if (!running) {
			LOG.debug("snapshotState() called on closed source");
			return null;
		}

		HashMap<KafkaTopicPartition, Long> currentOffsets = new HashMap<>();
		for (Map.Entry<KafkaTopicPartition, KafkaPartitionState> entry: partitionState.entrySet()) {
			currentOffsets.put(entry.getKey(), entry.getValue().getOffset());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Snapshotting state. Offsets: {}, checkpoint id: {}, timestamp: {}",
					KafkaTopicPartition.toString(currentOffsets), checkpointId, checkpointTimestamp);
		}

		// the map cannot be asynchronously updated, because only one checkpoint call can happen
		// on this function at a time: either snapshotState() or notifyCheckpointComplete()
		pendingCheckpoints.put(checkpointId, currentOffsets);
			
		while (pendingCheckpoints.size() > MAX_NUM_PENDING_CHECKPOINTS) {
			pendingCheckpoints.remove(0);
		}

		return currentOffsets;
	}

	@Override
	public void restoreState(HashMap<KafkaTopicPartition, Long> restoredOffsets) {
		LOG.info("Setting restore state in Kafka");
		restoreToOffset = restoredOffsets;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (partitionState == null) {
			LOG.debug("notifyCheckpointComplete() called on uninitialized source");
			return;
		}
		if (!running) {
			LOG.debug("notifyCheckpointComplete() called on closed source");
			return;
		}
		
		// only one commit operation must be in progress
		if (LOG.isDebugEnabled()) {
			LOG.debug("Committing offsets externally for checkpoint {}", checkpointId);
		}

		try {
			HashMap<KafkaTopicPartition, Long> checkpointOffsets;
	
			// the map may be asynchronously updates when snapshotting state, so we synchronize
			synchronized (pendingCheckpoints) {
				final int posInMap = pendingCheckpoints.indexOf(checkpointId);
				if (posInMap == -1) {
					LOG.warn("Received confirmation for unknown checkpoint id {}", checkpointId);
					return;
				}

				//noinspection unchecked
				checkpointOffsets = (HashMap<KafkaTopicPartition, Long>) pendingCheckpoints.remove(posInMap);

				
				// remove older checkpoints in map
				for (int i = 0; i < posInMap; i++) {
					pendingCheckpoints.remove(0);
				}
			}
			if (checkpointOffsets == null || checkpointOffsets.size() == 0) {
				LOG.debug("Checkpoint state was empty.");
				return;
			}
			commitOffsets(checkpointOffsets);
		}
		catch (Exception e) {
			if (running) {
				throw e;
			}
			// else ignore exception if we are no longer running
		}
	}

	protected abstract void commitOffsets(HashMap<KafkaTopicPartition, Long> checkpointOffsets) throws Exception;


	@Override
	public TypeInformation<T> getProducedType() {
		return deserializer.getProducedType();
	}

	protected static <T> List<T> assignPartitions(List<T> partitions, int numConsumers, int consumerIndex) {
		checkArgument(numConsumers > 0);
		checkArgument(consumerIndex < numConsumers);

		List<T> partitionsToSub = new ArrayList<>();

		for (int i = 0; i < partitions.size(); i++) {
			if (i % numConsumers == consumerIndex) {
				partitionsToSub.add(partitions.get(i));
			}
		}
		return partitionsToSub;
	}

	/**
	 * Method to log partition information.
	 * @param partitionInfos List of subscribed partitions
	 */
	public static void logPartitionInfo(List<KafkaTopicPartition> partitionInfos) {
		Map<String, Integer> countPerTopic = new HashMap<>();
		for (KafkaTopicPartition partition : partitionInfos) {
			Integer count = countPerTopic.get(partition.getTopic());
			if (count == null) {
				count = 1;
			} else {
				count++;
			}
			countPerTopic.put(partition.getTopic(), count);
		}
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Integer> e : countPerTopic.entrySet()) {
			sb.append(e.getKey()).append(" (").append(e.getValue()).append("), ");
		}
		LOG.info("Consumer is going to read the following topics (with number of partitions): {}", sb.toString());
	}


}
