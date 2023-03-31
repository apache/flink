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
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.impl.StaticFileSplitEnumerator;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.ExceptionUtils;

import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.connectors.hive.HiveOptions.TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An improved {@link SplitEnumerator} for hive source with bounded / batch input. Comparing with
 * the {@link StaticFileSplitEnumerator}, This enumerator will generate splits at the partition
 * level, which helps to avoid Java's out-of-memory (OOM) issue.
 */
public class StaticHivePartitionSplitEnumerator<T extends Comparable<T>>
        implements SplitEnumerator<HiveSourceSplit, PendingSplitsCheckpoint<HiveSourceSplit>> {

    private static final Logger LOG =
            LoggerFactory.getLogger(StaticHivePartitionSplitEnumerator.class);

    private final Deque<HiveTablePartition> remainPartitions;

    private final Collection<Path> processedPaths;

    private final FileSplitAssigner splitAssigner;

    private final SplitEnumeratorContext<HiveSourceSplit> enumeratorContext;

    private final JobConf jobConf;

    private final int currentParallelism;

    public StaticHivePartitionSplitEnumerator(
            SplitEnumeratorContext<HiveSourceSplit> enumeratorContext,
            Deque<HiveTablePartition> allPartitions,
            FileSplitAssigner splitAssigner,
            JobConf jobConf) {
        this.splitAssigner = splitAssigner;
        this.remainPartitions = allPartitions;
        this.enumeratorContext = enumeratorContext;
        this.processedPaths = new ArrayList<>();
        this.jobConf = jobConf;
        this.currentParallelism = enumeratorContext.currentParallelism();
    }

    @Override
    public void start() {
        // nothing to do.
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String hostName) {

        if (LOG.isInfoEnabled()) {
            final String hostInfo =
                    hostName == null ? "(no host locality info)" : "(on host '" + hostName + "')";
            LOG.info("Subtask {} {} is requesting a file source split", subtaskId, hostInfo);
        }

        final Optional<FileSourceSplit> nextSplit = splitAssigner.getNext(hostName);
        if (nextSplit.isPresent()) {
            final FileSourceSplit split = nextSplit.get();
            enumeratorContext.assignSplit((HiveSourceSplit) split, subtaskId);
            processedPaths.add(split.path());
            LOG.info("Assigned split to subtask {} : {}", subtaskId, split);
        } else {
            if (remainPartitions.isEmpty()) {
                enumeratorContext.signalNoMoreSplits(subtaskId);
                LOG.info("No more splits available for subtask {}", subtaskId);
            } else {
                LOG.info("Generating more splits for subtask {}", subtaskId);
                assignSplits();
                handleSplitRequest(subtaskId, hostName);
            }
        }
    }

    private void assignSplits() {
        int threadNumToSplitHiveFile = getThreadNumToSplitHiveFile();
        checkState(
                threadNumToSplitHiveFile > 0,
                TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM.description());
        List<HiveTablePartition> partitions = new ArrayList<>();
        while (!remainPartitions.isEmpty() && threadNumToSplitHiveFile > 0) {
            partitions.add(remainPartitions.poll());
            threadNumToSplitHiveFile--;
        }
        try {
            Collection<FileSourceSplit> splits =
                    new ArrayList<>(
                            HiveSourceFileEnumerator.createInputSplits(
                                    currentParallelism, partitions, jobConf, false));
            splitAssigner.addSplits(splits);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to create input splits.");
        }
    }

    private int getThreadNumToSplitHiveFile() {
        return jobConf.getInt(
                TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM.key(),
                TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM.defaultValue());
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.error("Received unrecognized event: {}", sourceEvent);
    }

    @Override
    public void addSplitsBack(List<HiveSourceSplit> splits, int subtaskId) {
        LOG.debug("Adding splits back: {}", splits);
        splitAssigner.addSplits(new ArrayList<>(splits));
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public PendingSplitsCheckpoint<HiveSourceSplit> snapshotState(long checkpointId)
            throws Exception {
        // remain split
        Collection<HiveSourceSplit> remainingSplits = new ArrayList<>();
        for (FileSourceSplit fileSourceSplit : splitAssigner.remainingSplits()) {
            remainingSplits.add((HiveSourceSplit) fileSourceSplit);
        }
        // processed partitions
        return PendingSplitsCheckpoint.fromCollectionSnapshot(
                new ArrayList<>(remainingSplits), processedPaths);
    }

    @Override
    public void close() throws IOException {
        // nothing to do
    }
}
