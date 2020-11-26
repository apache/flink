/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.source.reader;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kafka.source.reader.fetcher.KafkaSourceFetcherManager;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * The source reader for Kafka partitions.
 */
public class KafkaSourceReader<T>
		extends SingleThreadMultiplexSourceReaderBase<Tuple3<T, Long, Long>, T, KafkaPartitionSplit, KafkaPartitionSplitState> {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceReader.class);
	// These maps need to be concurrent because it will be accessed by both the main thread
	// and the split fetcher thread in the callback.
	private final SortedMap<Long, Map<TopicPartition, OffsetAndMetadata>> offsetsToCommit;
	private final ConcurrentMap<TopicPartition, OffsetAndMetadata> offsetsOfFinishedSplits;

	public KafkaSourceReader(
			FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple3<T, Long, Long>>> elementsQueue,
			Supplier<KafkaPartitionSplitReader<T>> splitReaderSupplier,
			RecordEmitter<Tuple3<T, Long, Long>, T, KafkaPartitionSplitState> recordEmitter,
			Configuration config,
			SourceReaderContext context) {
		super(
			elementsQueue,
			new KafkaSourceFetcherManager<>(elementsQueue, splitReaderSupplier::get),
			recordEmitter,
			config,
			context);
		this.offsetsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
		this.offsetsOfFinishedSplits = new ConcurrentHashMap<>();
	}

	@Override
	protected void onSplitFinished(Map<String, KafkaPartitionSplitState> finishedSplitIds) {
		finishedSplitIds.forEach((ignored, splitState) -> {
			offsetsOfFinishedSplits.put(
				splitState.getTopicPartition(),
				new OffsetAndMetadata(splitState.getCurrentOffset()));
		});
	}

	@Override
	public List<KafkaPartitionSplit> snapshotState(long checkpointId) {
		List<KafkaPartitionSplit> splits = super.snapshotState(checkpointId);
		if (splits.isEmpty() && offsetsOfFinishedSplits.isEmpty()) {
			offsetsToCommit.put(checkpointId, Collections.emptyMap());
		} else {
			Map<TopicPartition, OffsetAndMetadata> offsetsMap =
				offsetsToCommit.computeIfAbsent(checkpointId, id -> new HashMap<>());
			// Put the offsets of the active splits.
			for (KafkaPartitionSplit split : splits) {
				offsetsMap.put(
						split.getTopicPartition(),
						new OffsetAndMetadata(split.getStartingOffset(), null));
			}
			// Put offsets of all the finished splits.
			offsetsMap.putAll(offsetsOfFinishedSplits);
		}
		return splits;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		LOG.info("Committing offsets for checkpoint {}", checkpointId);
		((KafkaSourceFetcherManager<T>) splitFetcherManager).commitOffsets(
				offsetsToCommit.get(checkpointId),
				(ignored, e) -> {
					if (e != null) {
						LOG.warn("Failed to commit consumer offsets for checkpoint {}", checkpointId, e);
					} else {
						LOG.debug("Successfully committed offsets for checkpoint {}", checkpointId);
						// If the finished topic partition has been committed, we remove it
						// from the offsets of finsihed splits map.
						Map<TopicPartition, OffsetAndMetadata> committedPartitions =
							offsetsToCommit.get(checkpointId);
						offsetsOfFinishedSplits.entrySet().removeIf(
							entry -> committedPartitions.containsKey(entry.getKey()));
						while (!offsetsToCommit.isEmpty() && offsetsToCommit.firstKey() <= checkpointId) {
							offsetsToCommit.remove(offsetsToCommit.firstKey());
						}
					}
				});
	}

	@Override
	protected KafkaPartitionSplitState initializedState(KafkaPartitionSplit split) {
		return new KafkaPartitionSplitState(split);
	}

	@Override
	protected KafkaPartitionSplit toSplitType(String splitId, KafkaPartitionSplitState splitState) {
		return splitState.toKafkaPartitionSplit();
	}

	// ------------------------

	@VisibleForTesting
	SortedMap<Long, Map<TopicPartition, OffsetAndMetadata>> getOffsetsToCommit() {
		return offsetsToCommit;
	}

	@VisibleForTesting
	int getNumAliveFetchers() {
		return splitFetcherManager.getNumAliveFetchers();
	}
}
