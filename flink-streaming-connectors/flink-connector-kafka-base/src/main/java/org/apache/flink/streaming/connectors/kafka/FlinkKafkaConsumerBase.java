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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
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
		implements CheckpointListener, CheckpointedAsynchronously<HashMap<KafkaTopicPartition, Long>>, ResultTypeQueryable<T> {

	// ------------------------------------------------------------------------

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaConsumerBase.class);

	private static final long serialVersionUID = -6272159445203409112L;

	/** The maximum number of pending non-committed checkpoints to track, to avoid memory leaks */
	public static final int MAX_NUM_PENDING_CHECKPOINTS = 100;


	/** The schema to convert between Kafka#s byte messages, and Flink's objects */
	protected final KeyedDeserializationSchema<T> deserializer;

	// ------  Runtime State  -------

	/** Data for pending but uncommitted checkpoints */
	protected final LinkedMap pendingCheckpoints = new LinkedMap();

	/** The offsets of the last returned elements */
	protected transient HashMap<KafkaTopicPartition, Long> offsetsState;

	/** The offsets to restore to, if the consumer restores state from a checkpoint */
	protected transient HashMap<KafkaTopicPartition, Long> restoreToOffset;

	/** Flag indicating whether the consumer is still running **/
	protected volatile boolean running = true;

	// ------------------------------------------------------------------------


	/**
	 * Creates a new Flink Kafka Consumer, using the given type of fetcher and offset handler.
	 *
	 * <p>To determine which kink of fetcher and offset handler to use, please refer to the docs
	 * at the beginnign of this class.</p>
	 *
	 * @param deserializer
	 *           The deserializer to turn raw byte messages into Java/Scala objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkKafkaConsumerBase(KeyedDeserializationSchema<T> deserializer, Properties props) {
		this.deserializer = requireNonNull(deserializer, "valueDeserializer");
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and restore
	// ------------------------------------------------------------------------

	@Override
	public HashMap<KafkaTopicPartition, Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		if (offsetsState == null) {
			LOG.debug("snapshotState() requested on not yet opened source; returning null.");
			return null;
		}
		if (!running) {
			LOG.debug("snapshotState() called on closed source");
			return null;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Snapshotting state. Offsets: {}, checkpoint id: {}, timestamp: {}",
					KafkaTopicPartition.toString(offsetsState), checkpointId, checkpointTimestamp);
		}

		// the use of clone() is okay here is okay, we just need a new map, the keys are not changed
		//noinspection unchecked
		HashMap<KafkaTopicPartition, Long> currentOffsets = (HashMap<KafkaTopicPartition, Long>) offsetsState.clone();

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
		if (offsetsState == null) {
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
		LOG.info("Consumer is going to read the following topics (with number of partitions): ", sb.toString());
	}


}
