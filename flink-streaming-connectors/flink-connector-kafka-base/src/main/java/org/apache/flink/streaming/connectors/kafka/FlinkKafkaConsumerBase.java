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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class of all Flink Kafka Consumer data sources.
 * This implements the common behavior across all Kafka versions.
 * 
 * <p>The Kafka version specific behavior is defined mainly in the specific subclasses of the
 * {@link AbstractFetcher}.
 * 
 * @param <T> The type of records produced by this data source
 */
public abstract class FlinkKafkaConsumerBase<T> extends RichParallelSourceFunction<T> implements 
		CheckpointListener,
		ResultTypeQueryable<T>,
		CheckpointedFunction {
	private static final long serialVersionUID = -6272159445203409112L;

	protected static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaConsumerBase.class);
	
	/** The maximum number of pending non-committed checkpoints to track, to avoid memory leaks */
	public static final int MAX_NUM_PENDING_CHECKPOINTS = 100;

	/** Boolean configuration key to disable metrics tracking **/
	public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

	// ------------------------------------------------------------------------
	//  configuration state, set on the client relevant for all subtasks
	// ------------------------------------------------------------------------

	private final List<String> topics;
	
	/** The schema to convert between Kafka's byte messages, and Flink's objects */
	protected final KeyedDeserializationSchema<T> deserializer;

	/** The set of topic partitions that the source will read */
	protected List<KafkaTopicPartition> subscribedPartitions;
	
	/** Optional timestamp extractor / watermark generator that will be run per Kafka partition,
	 * to exploit per-partition timestamp characteristics.
	 * The assigner is kept in serialized form, to deserialize it into multiple copies */
	private SerializedValue<AssignerWithPeriodicWatermarks<T>> periodicWatermarkAssigner;
	
	/** Optional timestamp extractor / watermark generator that will be run per Kafka partition,
	 * to exploit per-partition timestamp characteristics. 
	 * The assigner is kept in serialized form, to deserialize it into multiple copies */
	private SerializedValue<AssignerWithPunctuatedWatermarks<T>> punctuatedWatermarkAssigner;

	private transient OperatorStateStore stateStore;

	// ------------------------------------------------------------------------
	//  runtime state (used individually by each parallel subtask) 
	// ------------------------------------------------------------------------
	
	/** Data for pending but uncommitted checkpoints */
	private final LinkedMap pendingCheckpoints = new LinkedMap();

	/** The fetcher implements the connections to the Kafka brokers */
	private transient volatile AbstractFetcher<T, ?> kafkaFetcher;
	
	/** The offsets to restore to, if the consumer restores state from a checkpoint */
	private transient volatile HashMap<KafkaTopicPartition, Long> restoreToOffset;
	
	/** Flag indicating whether the consumer is still running **/
	private volatile boolean running = true;

	// ------------------------------------------------------------------------

	/**
	 * Base constructor.
	 *
	 * @param deserializer
	 *           The deserializer to turn raw byte messages into Java/Scala objects.
	 */
	public FlinkKafkaConsumerBase(List<String> topics, KeyedDeserializationSchema<T> deserializer) {
		this.topics = checkNotNull(topics);
		checkArgument(topics.size() > 0, "You have to define at least one topic.");
		this.deserializer = checkNotNull(deserializer, "valueDeserializer");
	}

	/**
	 * This method must be called from the subclasses, to set the list of all subscribed partitions
	 * that this consumer will fetch from (across all subtasks).
	 * 
	 * @param allSubscribedPartitions The list of all partitions that all subtasks together should fetch from.
	 */
	protected void setSubscribedPartitions(List<KafkaTopicPartition> allSubscribedPartitions) {
		checkNotNull(allSubscribedPartitions);
		this.subscribedPartitions = Collections.unmodifiableList(allSubscribedPartitions);
	}

	// ------------------------------------------------------------------------
	//  Configuration
	// ------------------------------------------------------------------------
	
	/**
	 * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks in a punctuated manner.
	 * The watermark extractor will run per Kafka partition, watermarks will be merged across partitions
	 * in the same way as in the Flink runtime, when streams are merged.
	 * 
	 * <p>When a subtask of a FlinkKafkaConsumer source reads multiple Kafka partitions,
	 * the streams from the partitions are unioned in a "first come first serve" fashion. Per-partition
	 * characteristics are usually lost that way. For example, if the timestamps are strictly ascending
	 * per Kafka partition, they will not be strictly ascending in the resulting Flink DataStream, if the
	 * parallel source subtask reads more that one partition.
	 * 
	 * <p>Running timestamp extractors / watermark generators directly inside the Kafka source, per Kafka
	 * partition, allows users to let them exploit the per-partition characteristics.
	 * 
	 * <p>Note: One can use either an {@link AssignerWithPunctuatedWatermarks} or an
	 * {@link AssignerWithPeriodicWatermarks}, not both at the same time.
	 * 
	 * @param assigner The timestamp assigner / watermark generator to use.
	 * @return The consumer object, to allow function chaining.   
	 */
	public FlinkKafkaConsumerBase<T> assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks<T> assigner) {
		checkNotNull(assigner);
		
		if (this.periodicWatermarkAssigner != null) {
			throw new IllegalStateException("A periodic watermark emitter has already been set.");
		}
		try {
			this.punctuatedWatermarkAssigner = new SerializedValue<>(assigner);
			return this;
		} catch (Exception e) {
			throw new IllegalArgumentException("The given assigner is not serializable", e);
		}
	}

	/**
	 * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks in a punctuated manner.
	 * The watermark extractor will run per Kafka partition, watermarks will be merged across partitions
	 * in the same way as in the Flink runtime, when streams are merged.
	 *
	 * <p>When a subtask of a FlinkKafkaConsumer source reads multiple Kafka partitions,
	 * the streams from the partitions are unioned in a "first come first serve" fashion. Per-partition
	 * characteristics are usually lost that way. For example, if the timestamps are strictly ascending
	 * per Kafka partition, they will not be strictly ascending in the resulting Flink DataStream, if the
	 * parallel source subtask reads more that one partition.
	 *
	 * <p>Running timestamp extractors / watermark generators directly inside the Kafka source, per Kafka
	 * partition, allows users to let them exploit the per-partition characteristics.
	 *
	 * <p>Note: One can use either an {@link AssignerWithPunctuatedWatermarks} or an
	 * {@link AssignerWithPeriodicWatermarks}, not both at the same time.
	 *
	 * @param assigner The timestamp assigner / watermark generator to use.
	 * @return The consumer object, to allow function chaining.   
	 */
	public FlinkKafkaConsumerBase<T> assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks<T> assigner) {
		checkNotNull(assigner);
		
		if (this.punctuatedWatermarkAssigner != null) {
			throw new IllegalStateException("A punctuated watermark emitter has already been set.");
		}
		try {
			this.periodicWatermarkAssigner = new SerializedValue<>(assigner);
			return this;
		} catch (Exception e) {
			throw new IllegalArgumentException("The given assigner is not serializable", e);
		}
	}

	// ------------------------------------------------------------------------
	//  Work methods
	// ------------------------------------------------------------------------

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {
		if (subscribedPartitions == null) {
			throw new Exception("The partitions were not set for the consumer");
		}

		// we need only do work, if we actually have partitions assigned
		if (!subscribedPartitions.isEmpty()) {

			// (1) create the fetcher that will communicate with the Kafka brokers
			final AbstractFetcher<T, ?> fetcher = createFetcher(
					sourceContext, subscribedPartitions,
					periodicWatermarkAssigner, punctuatedWatermarkAssigner,
					(StreamingRuntimeContext) getRuntimeContext());

			// (2) set the fetcher to the restored checkpoint offsets
			if (restoreToOffset != null) {
				fetcher.restoreOffsets(restoreToOffset);
			}

			// publish the reference, for snapshot-, commit-, and cancel calls
			// IMPORTANT: We can only do that now, because only now will calls to
			//            the fetchers 'snapshotCurrentState()' method return at least
			//            the restored offsets
			this.kafkaFetcher = fetcher;
			if (!running) {
				return;
			}
			
			// (3) run the fetcher' main work method
			fetcher.runFetchLoop();
		}
		else {
			// this source never completes, so emit a Long.MAX_VALUE watermark
			// to not block watermark forwarding
			sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));

			// wait until this is canceled
			final Object waitLock = new Object();
			while (running) {
				try {
					//noinspection SynchronizationOnLocalVariableOrMethodParameter
					synchronized (waitLock) {
						waitLock.wait();
					}
				}
				catch (InterruptedException e) {
					if (!running) {
						// restore the interrupted state, and fall through the loop
						Thread.currentThread().interrupt();
					}
				}
			}
		}
	}

	@Override
	public void cancel() {
		// set ourselves as not running
		running = false;
		
		// abort the fetcher, if there is one
		if (kafkaFetcher != null) {
			kafkaFetcher.cancel();
		}

		// there will be an interrupt() call to the main thread anyways
	}

	@Override
	public void open(Configuration configuration) {
		List<KafkaTopicPartition> kafkaTopicPartitions = getKafkaPartitions(topics);

		if (kafkaTopicPartitions != null) {
			assignTopicPartitions(kafkaTopicPartitions);
		}
	}

	@Override
	public void close() throws Exception {
		// pretty much the same logic as cancelling
		try {
			cancel();
		} finally {
			super.close();
		}
	}
	
	// ------------------------------------------------------------------------
	//  Checkpoint and restore
	// ------------------------------------------------------------------------

	@Override
	public void initializeState(OperatorStateStore stateStore) throws Exception {

		this.stateStore = stateStore;

		ListState<Serializable> offsets =
				stateStore.getSerializableListState(OperatorStateStore.DEFAULT_OPERATOR_STATE_NAME);

		restoreToOffset = new HashMap<>();

		for (Serializable serializable : offsets.get()) {
			@SuppressWarnings("unchecked")
			Tuple2<KafkaTopicPartition, Long> kafkaOffset = (Tuple2<KafkaTopicPartition, Long>) serializable;
			restoreToOffset.put(kafkaOffset.f0, kafkaOffset.f1);
		}

		LOG.info("Setting restore state in the FlinkKafkaConsumer: {}", restoreToOffset);
	}

	@Override
	public void prepareSnapshot(long checkpointId, long timestamp) throws Exception {
		if (!running) {
			LOG.debug("storeOperatorState() called on closed source");
		} else {

			ListState<Serializable> listState =
					stateStore.getSerializableListState(OperatorStateStore.DEFAULT_OPERATOR_STATE_NAME);
			listState.clear();

			final AbstractFetcher<?, ?> fetcher = this.kafkaFetcher;
			if (fetcher == null) {
				// the fetcher has not yet been initialized, which means we need to return the
				// originally restored offsets or the assigned partitions

				if (restoreToOffset != null) {
					// the map cannot be asynchronously updated, because only one checkpoint call can happen
					// on this function at a time: either snapshotState() or notifyCheckpointComplete()
					pendingCheckpoints.put(checkpointId, restoreToOffset);

					// truncate the map, to prevent infinite growth
					while (pendingCheckpoints.size() > MAX_NUM_PENDING_CHECKPOINTS) {
						pendingCheckpoints.remove(0);
					}

					for (Map.Entry<KafkaTopicPartition, Long> kafkaTopicPartitionLongEntry : restoreToOffset.entrySet()) {
						listState.add(Tuple2.of(kafkaTopicPartitionLongEntry.getKey(), kafkaTopicPartitionLongEntry.getValue()));
					}
				} else if (subscribedPartitions != null) {
					for (KafkaTopicPartition subscribedPartition : subscribedPartitions) {
						listState.add(Tuple2.of(subscribedPartition, KafkaTopicPartitionState.OFFSET_NOT_SET));
					}
				}
			} else {
				HashMap<KafkaTopicPartition, Long> currentOffsets = fetcher.snapshotCurrentState();

				// the map cannot be asynchronously updated, because only one checkpoint call can happen
				// on this function at a time: either snapshotState() or notifyCheckpointComplete()
				pendingCheckpoints.put(checkpointId, currentOffsets);

				// truncate the map, to prevent infinite growth
				while (pendingCheckpoints.size() > MAX_NUM_PENDING_CHECKPOINTS) {
					pendingCheckpoints.remove(0);
				}

				for (Map.Entry<KafkaTopicPartition, Long> kafkaTopicPartitionLongEntry : currentOffsets.entrySet()) {
					listState.add(Tuple2.of(kafkaTopicPartitionLongEntry.getKey(), kafkaTopicPartitionLongEntry.getValue()));
				}
			}
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (!running) {
			LOG.debug("notifyCheckpointComplete() called on closed source");
			return;
		}

		final AbstractFetcher<?, ?> fetcher = this.kafkaFetcher;
		if (fetcher == null) {
			LOG.debug("notifyCheckpointComplete() called on uninitialized source");
			return;
		}
		
		// only one commit operation must be in progress
		if (LOG.isDebugEnabled()) {
			LOG.debug("Committing offsets to Kafka/ZooKeeper for checkpoint " + checkpointId);
		}

		try {
			final int posInMap = pendingCheckpoints.indexOf(checkpointId);
			if (posInMap == -1) {
				LOG.warn("Received confirmation for unknown checkpoint id {}", checkpointId);
				return;
			}

			@SuppressWarnings("unchecked")
			HashMap<KafkaTopicPartition, Long> checkpointOffsets = 
					(HashMap<KafkaTopicPartition, Long>) pendingCheckpoints.remove(posInMap);

			// remove older checkpoints in map
			for (int i = 0; i < posInMap; i++) {
				pendingCheckpoints.remove(0);
			}

			if (checkpointOffsets == null || checkpointOffsets.size() == 0) {
				LOG.debug("Checkpoint state was empty.");
				return;
			}
			fetcher.commitSpecificOffsetsToKafka(checkpointOffsets);
		}
		catch (Exception e) {
			if (running) {
				throw e;
			}
			// else ignore exception if we are no longer running
		}
	}

	// ------------------------------------------------------------------------
	//  Kafka Consumer specific methods
	// ------------------------------------------------------------------------
	
	/**
	 * Creates the fetcher that connect to the Kafka brokers, pulls data, deserialized the
	 * data, and emits it into the data streams.
	 * 
	 * @param sourceContext The source context to emit data to.
	 * @param thisSubtaskPartitions The set of partitions that this subtask should handle.
	 * @param watermarksPeriodic Optional, a serialized timestamp extractor / periodic watermark generator.
	 * @param watermarksPunctuated Optional, a serialized timestamp extractor / punctuated watermark generator.
	 * @param runtimeContext The task's runtime context.
	 * 
	 * @return The instantiated fetcher
	 * 
	 * @throws Exception The method should forward exceptions
	 */
	protected abstract AbstractFetcher<T, ?> createFetcher(
			SourceContext<T> sourceContext,
			List<KafkaTopicPartition> thisSubtaskPartitions,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			StreamingRuntimeContext runtimeContext) throws Exception;

	protected abstract List<KafkaTopicPartition> getKafkaPartitions(List<String> topics);
	
	// ------------------------------------------------------------------------
	//  ResultTypeQueryable methods 
	// ------------------------------------------------------------------------
	
	@Override
	public TypeInformation<T> getProducedType() {
		return deserializer.getProducedType();
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private void assignTopicPartitions(List<KafkaTopicPartition> kafkaTopicPartitions) {
		subscribedPartitions = new ArrayList<>();

		if (restoreToOffset != null) {
			for (KafkaTopicPartition kafkaTopicPartition : kafkaTopicPartitions) {
				if (restoreToOffset.containsKey(kafkaTopicPartition)) {
					subscribedPartitions.add(kafkaTopicPartition);
				}
			}
		} else {
			Collections.sort(kafkaTopicPartitions, new Comparator<KafkaTopicPartition>() {
				@Override
				public int compare(KafkaTopicPartition o1, KafkaTopicPartition o2) {
					int topicComparison = o1.getTopic().compareTo(o2.getTopic());

					if (topicComparison == 0) {
						return o1.getPartition() - o2.getPartition();
					} else {
						return topicComparison;
					}
				}
			});

			for (int i = getRuntimeContext().getIndexOfThisSubtask(); i < kafkaTopicPartitions.size(); i += getRuntimeContext().getNumberOfParallelSubtasks()) {
				subscribedPartitions.add(kafkaTopicPartitions.get(i));
			}
		}
	}

	/**
	 * Selects which of the given partitions should be handled by a specific consumer,
	 * given a certain number of consumers.
	 * 
	 * @param allPartitions The partitions to select from
	 * @param numConsumers The number of consumers
	 * @param consumerIndex The index of the specific consumer
	 * 
	 * @return The sublist of partitions to be handled by that consumer.
	 */
	protected static List<KafkaTopicPartition> assignPartitions(
			List<KafkaTopicPartition> allPartitions,
			int numConsumers, int consumerIndex) {
		final List<KafkaTopicPartition> thisSubtaskPartitions = new ArrayList<>(
				allPartitions.size() / numConsumers + 1);

		for (int i = 0; i < allPartitions.size(); i++) {
			if (i % numConsumers == consumerIndex) {
				thisSubtaskPartitions.add(allPartitions.get(i));
			}
		}
		
		return thisSubtaskPartitions;
	}
	
	/**
	 * Logs the partition information in INFO level.
	 * 
	 * @param logger The logger to log to.
	 * @param partitionInfos List of subscribed partitions
	 */
	protected static void logPartitionInfo(Logger logger, List<KafkaTopicPartition> partitionInfos) {
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
		StringBuilder sb = new StringBuilder(
				"Consumer is going to read the following topics (with number of partitions): ");
		
		for (Map.Entry<String, Integer> e : countPerTopic.entrySet()) {
			sb.append(e.getKey()).append(" (").append(e.getValue()).append("), ");
		}
		
		logger.info(sb.toString());
	}
}
