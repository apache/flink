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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connectors.hive.read.HiveContinuousPartitionContext;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.filesystem.ContinuousPartitionFetcher;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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
import java.util.concurrent.Callable;

/** A continuously monitoring {@link SplitEnumerator} for hive source. */
public class ContinuousHiveSplitEnumerator<T extends Comparable<T>>
        implements SplitEnumerator<HiveSourceSplit, PendingSplitsCheckpoint<HiveSourceSplit>> {

    private static final Logger LOG = LoggerFactory.getLogger(ContinuousHiveSplitEnumerator.class);

    private final SplitEnumeratorContext<HiveSourceSplit> enumeratorContext;
    private final LinkedHashMap<Integer, String> readersAwaitingSplit;
    private final FileSplitAssigner splitAssigner;
    private final long discoveryInterval;

    private final HiveTableSource.HiveContinuousPartitionFetcherContext<T> fetcherContext;

    // the maximum partition read offset seen so far
    private T currentReadOffset;
    // the partitions that have been processed for the current read offset
    private Collection<List<String>> seenPartitionsSinceOffset;
    private final PartitionMonitor<T> monitor;

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
        this.seenPartitionsSinceOffset = new ArrayList<>(seenPartitionsSinceOffset);
        this.splitAssigner = splitAssigner;
        this.discoveryInterval = discoveryInterval;
        this.fetcherContext = fetcherContext;
        readersAwaitingSplit = new LinkedHashMap<>();
        monitor =
                new PartitionMonitor<>(
                        currentReadOffset,
                        seenPartitionsSinceOffset,
                        tablePath,
                        jobConf,
                        fetcher,
                        fetcherContext);
    }

    @Override
    public void start() {
        try {
            fetcherContext.open();
            enumeratorContext.callAsync(
                    monitor, this::handleNewSplits, discoveryInterval, discoveryInterval);
        } catch (Exception e) {
            throw new FlinkHiveException("Failed to start continuous split enumerator", e);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String hostName) {
        readersAwaitingSplit.put(subtaskId, hostName);
        assignSplits();
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.error("Received unrecognized event: {}", sourceEvent);
    }

    @Override
    public void addSplitsBack(List<HiveSourceSplit> splits, int subtaskId) {
        LOG.debug("Continuous Hive Source Enumerator adds splits back: {}", splits);
        splitAssigner.addSplits(new ArrayList<>(splits));
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public PendingSplitsCheckpoint<HiveSourceSplit> snapshotState(long checkpointId)
            throws Exception {
        Collection<HiveSourceSplit> remainingSplits =
                (Collection<HiveSourceSplit>) (Collection<?>) splitAssigner.remainingSplits();
        return new ContinuousHivePendingSplitsCheckpoint(
                remainingSplits, currentReadOffset, seenPartitionsSinceOffset);
    }

    @Override
    public void close() throws IOException {
        try {
            fetcherContext.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void handleNewSplits(NewSplitsAndState<T> newSplitsAndState, Throwable error) {
        if (error != null) {
            // we need to failover because the worker thread is stateful
            throw new FlinkHiveException("Failed to enumerate files", error);
        }
        this.currentReadOffset = newSplitsAndState.offset;
        this.seenPartitionsSinceOffset = newSplitsAndState.seenPartitions;
        splitAssigner.addSplits(new ArrayList<>(newSplitsAndState.newSplits));
        assignSplits();
    }

    private void assignSplits() {
        final Iterator<Map.Entry<Integer, String>> awaitingReader =
                readersAwaitingSplit.entrySet().iterator();
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
    }

    static class PartitionMonitor<T extends Comparable<T>>
            implements Callable<NewSplitsAndState<T>> {

        // keep these locally so that we don't need to share state with main thread
        private T currentReadOffset;
        private final Set<List<String>> seenPartitionsSinceOffset;

        private final ObjectPath tablePath;
        private final JobConf jobConf;
        private final ContinuousPartitionFetcher<Partition, T> fetcher;
        private final HiveContinuousPartitionContext<Partition, T> fetcherContext;

        PartitionMonitor(
                T currentReadOffset,
                Collection<List<String>> seenPartitionsSinceOffset,
                ObjectPath tablePath,
                JobConf jobConf,
                ContinuousPartitionFetcher<Partition, T> fetcher,
                HiveContinuousPartitionContext<Partition, T> fetcherContext) {
            this.currentReadOffset = currentReadOffset;
            this.seenPartitionsSinceOffset = new HashSet<>(seenPartitionsSinceOffset);
            this.tablePath = tablePath;
            this.jobConf = jobConf;
            this.fetcher = fetcher;
            this.fetcherContext = fetcherContext;
        }

        @Override
        public NewSplitsAndState<T> call() throws Exception {
            List<Tuple2<Partition, T>> partitions =
                    fetcher.fetchPartitions(fetcherContext, currentReadOffset);
            if (partitions.isEmpty()) {
                return new NewSplitsAndState<>(
                        Collections.emptyList(), currentReadOffset, seenPartitionsSinceOffset);
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
                    if (offset.compareTo(currentReadOffset) >= 0) {
                        nextSeen.add(partSpec);
                    }
                    if (offset.compareTo(maxOffset) >= 0) {
                        maxOffset = offset;
                    }
                    LOG.info(
                            "Found new partition {} of table {}, generating splits for it",
                            partSpec,
                            tablePath.getFullName());
                    newSplits.addAll(
                            HiveSourceFileEnumerator.createInputSplits(
                                    0,
                                    Collections.singletonList(
                                            fetcherContext.toHiveTablePartition(partition)),
                                    jobConf));
                }
            }
            currentReadOffset = maxOffset;
            if (!nextSeen.isEmpty()) {
                seenPartitionsSinceOffset.clear();
                seenPartitionsSinceOffset.addAll(nextSeen);
            }
            return new NewSplitsAndState<>(newSplits, currentReadOffset, seenPartitionsSinceOffset);
        }
    }

    /** The result passed from monitor thread to main thread. */
    static class NewSplitsAndState<T extends Comparable<T>> {
        private final T offset;
        private final Collection<List<String>> seenPartitions;
        private final Collection<HiveSourceSplit> newSplits;

        private NewSplitsAndState(
                Collection<HiveSourceSplit> newSplits,
                T offset,
                Collection<List<String>> seenPartitions) {
            this.newSplits = newSplits;
            this.offset = offset;
            this.seenPartitions = new ArrayList<>(seenPartitions);
        }

        @VisibleForTesting
        Collection<List<String>> getSeenPartitions() {
            return seenPartitions;
        }
    }
}
