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

package org.apache.flink.connectors.hive;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.event.RequestSplitEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.filesystem.ContinuousPartitionFetcher;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A continuously monitoring {@link SplitEnumerator} for hive source.
 */
public class ContinuousHiveSplitEnumerator<T extends Comparable<T>> implements SplitEnumerator<HiveSourceSplit, PendingSplitsCheckpoint<HiveSourceSplit>> {

	private static final Logger LOG = LoggerFactory.getLogger(ContinuousHiveSplitEnumerator.class);

	private final SplitEnumeratorContext<HiveSourceSplit> enumeratorContext;
	private final LinkedHashMap<Integer, String> readersAwaitingSplit;
	private final FileSplitAssigner splitAssigner;
	private final long discoveryInterval;

	private final JobConf jobConf;
	private final ObjectPath tablePath;

	private final ContinuousPartitionFetcher<Partition, T> fetcher;
	private final HiveTableSource.HiveContinuousPartitionFetcherContext<T> fetcherContext;

	private final ReadWriteLock stateLock = new ReentrantReadWriteLock();
	// the maximum partition read offset seen so far
	private volatile T currentReadOffset;
	// the partitions that have been processed for the current read offset
	private final Set<List<String>> seenPartitionsSinceOffset;

	public ContinuousHiveSplitEnumerator(
			SplitEnumeratorContext<HiveSourceSplit> enumeratorContext,
			T currentReadOffset,
			Collection<List<String>> seenPartitionsSinceOffset,
			FileSplitAssigner splitAssigner,
			long discoveryInterval,
			JobConf jobConf,
			ObjectPath tablePath,
			ContinuousPartitionFetcher<Partition, T> fetcher,
			HiveTableSource.HiveContinuousPartitionFetcherContext<T> fetcherContext) {
		this.enumeratorContext = enumeratorContext;
		this.currentReadOffset = currentReadOffset;
		this.seenPartitionsSinceOffset = new HashSet<>(seenPartitionsSinceOffset);
		this.splitAssigner = splitAssigner;
		this.discoveryInterval = discoveryInterval;
		this.jobConf = jobConf;
		this.tablePath = tablePath;
		this.fetcher = fetcher;
		this.fetcherContext = fetcherContext;
		readersAwaitingSplit = new LinkedHashMap<>();
	}

	@Override
	public void start() {
		try {
			fetcherContext.open();
			enumeratorContext.callAsync(
					this::monitorAndGetSplits,
					this::handleNewSplits,
					discoveryInterval,
					discoveryInterval);
		} catch (Exception e) {
			throw new FlinkHiveException("Failed to start continuous split enumerator", e);
		}
	}

	@Override
	public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
		if (sourceEvent instanceof RequestSplitEvent) {
			readersAwaitingSplit.put(subtaskId, ((RequestSplitEvent) sourceEvent).hostName());
			assignSplits();
		} else {
			LOG.error("Received unrecognized event: {}", sourceEvent);
		}
	}

	@Override
	public void addSplitsBack(List<HiveSourceSplit> splits, int subtaskId) {
		LOG.debug("Continuous Hive Source Enumerator adds splits back: {}", splits);
		stateLock.writeLock().lock();
		try {
			splitAssigner.addSplits(new ArrayList<>(splits));
		} finally {
			stateLock.writeLock().unlock();
		}
	}

	@Override
	public void addReader(int subtaskId) {
		// this source is purely lazy-pull-based, nothing to do upon registration
	}

	@Override
	public PendingSplitsCheckpoint<HiveSourceSplit> snapshotState() throws Exception {
		stateLock.readLock().lock();
		try {
			Collection<HiveSourceSplit> remainingSplits = (Collection<HiveSourceSplit>) (Collection<?>) splitAssigner.remainingSplits();
			return new ContinuousHivePendingSplitsCheckpoint(remainingSplits, currentReadOffset, seenPartitionsSinceOffset);
		} finally {
			stateLock.readLock().unlock();
		}
	}

	@Override
	public void close() throws IOException {
		try {
			fetcherContext.close();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	private Void monitorAndGetSplits() throws Exception {
		stateLock.writeLock().lock();
		try {
			List<Tuple2<Partition, T>> partitions = fetcher.fetchPartitions(fetcherContext, currentReadOffset);
			if (partitions.isEmpty()) {
				return null;
			}

			partitions.sort(Comparator.comparing(o -> o.f1));
			List<HiveSourceSplit> newSplits = new ArrayList<>();
			// the max offset of new partitions
			T maxOffset = currentReadOffset;
			Set<List<String>> nextSeen = new HashSet<>();
			for (Tuple2<Partition, T> tuple2 : partitions) {
				Partition partition = tuple2.f0;
				List<String> partSpec = partition.getValues();
				if (seenPartitionsSinceOffset.add(partSpec)) {
					T offset = tuple2.f1;
					if (offset.compareTo(currentReadOffset) > 0) {
						nextSeen.add(partSpec);
					}
					if (offset.compareTo(maxOffset) > 0) {
						maxOffset = offset;
					}
					LOG.info("Found new partition {} of table {}, generating splits for it",
							partSpec, tablePath.getFullName());
					newSplits.addAll(HiveSourceFileEnumerator.createInputSplits(
							0, Collections.singletonList(fetcherContext.toHiveTablePartition(partition)), jobConf));
				}
			}
			currentReadOffset = maxOffset;
			splitAssigner.addSplits(new ArrayList<>(newSplits));
			if (!nextSeen.isEmpty()) {
				seenPartitionsSinceOffset.clear();
				seenPartitionsSinceOffset.addAll(nextSeen);
			}
			return null;
		} finally {
			stateLock.writeLock().unlock();
		}
	}

	private void handleNewSplits(Void v, Throwable error) {
		if (error != null) {
			LOG.error("Failed to enumerate files", error);
			return;
		}
		assignSplits();
	}

	private void assignSplits() {
		final Iterator<Map.Entry<Integer, String>> awaitingReader = readersAwaitingSplit.entrySet().iterator();

		stateLock.writeLock().lock();
		try {
			while (awaitingReader.hasNext()) {
				final Map.Entry<Integer, String> nextAwaiting = awaitingReader.next();
				final String hostname = nextAwaiting.getValue();
				final int awaitingSubtask = nextAwaiting.getKey();
				final Optional<FileSourceSplit> nextSplit = splitAssigner.getNext(hostname);
				if (nextSplit.isPresent()) {
					enumeratorContext.assignSplit((HiveSourceSplit) nextSplit.get(), awaitingSubtask);
					awaitingReader.remove();
				} else {
					break;
				}
			}
		} finally {
			stateLock.writeLock().unlock();
		}
	}
}
