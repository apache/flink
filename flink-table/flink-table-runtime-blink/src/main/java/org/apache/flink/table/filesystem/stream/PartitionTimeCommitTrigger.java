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

package org.apache.flink.table.filesystem.stream;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.filesystem.PartitionTimeExtractor;
import org.apache.flink.util.StringUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.flink.table.filesystem.DefaultPartTimeExtractor.toMills;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_CLASS;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_KIND;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_DELAY;
import static org.apache.flink.table.utils.PartitionPathUtils.extractPartitionValues;

/**
 * Partition commit trigger by partition time and watermark, if 'watermark' > 'partition-time' +
 * 'delay', will commit the partition.
 *
 * <p>Compares watermark, and watermark is related to records and checkpoint, so we need store
 * watermark information for checkpoint.
 */
public class PartitionTimeCommitTrigger implements PartitionCommitTrigger {

    private static final ListStateDescriptor<List<String>> PENDING_PARTITIONS_STATE_DESC =
            new ListStateDescriptor<>(
                    "pending-partitions", new ListSerializer<>(StringSerializer.INSTANCE));

    private static final ListStateDescriptor<Map<Long, Long>> WATERMARKS_STATE_DESC =
            new ListStateDescriptor<>(
                    "checkpoint-id-to-watermark",
                    new MapSerializer<>(LongSerializer.INSTANCE, LongSerializer.INSTANCE));

    private final ListState<List<String>> pendingPartitionsState;
    private final Set<String> pendingPartitions;

    private final ListState<Map<Long, Long>> watermarksState;
    private final TreeMap<Long, Long> watermarks;
    private final PartitionTimeExtractor extractor;
    private final long commitDelay;
    private final List<String> partitionKeys;

    public PartitionTimeCommitTrigger(
            boolean isRestored,
            OperatorStateStore stateStore,
            Configuration conf,
            ClassLoader cl,
            List<String> partitionKeys)
            throws Exception {
        this.pendingPartitionsState = stateStore.getListState(PENDING_PARTITIONS_STATE_DESC);
        this.pendingPartitions = new HashSet<>();
        if (isRestored) {
            pendingPartitions.addAll(pendingPartitionsState.get().iterator().next());
        }

        this.partitionKeys = partitionKeys;
        this.commitDelay = conf.get(SINK_PARTITION_COMMIT_DELAY).toMillis();
        this.extractor =
                PartitionTimeExtractor.create(
                        cl,
                        conf.get(PARTITION_TIME_EXTRACTOR_KIND),
                        conf.get(PARTITION_TIME_EXTRACTOR_CLASS),
                        conf.get(PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN));

        this.watermarksState = stateStore.getListState(WATERMARKS_STATE_DESC);
        this.watermarks = new TreeMap<>();
        if (isRestored) {
            watermarks.putAll(watermarksState.get().iterator().next());
        }
    }

    @Override
    public void addPartition(String partition) {
        if (!StringUtils.isNullOrWhitespaceOnly(partition)) {
            this.pendingPartitions.add(partition);
        }
    }

    @Override
    public List<String> committablePartitions(long checkpointId) {
        if (!watermarks.containsKey(checkpointId)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Checkpoint(%d) has not been snapshot. The watermark information is: %s.",
                            checkpointId, watermarks));
        }

        long watermark = watermarks.get(checkpointId);
        watermarks.headMap(checkpointId, true).clear();

        List<String> needCommit = new ArrayList<>();
        Iterator<String> iter = pendingPartitions.iterator();
        while (iter.hasNext()) {
            String partition = iter.next();
            LocalDateTime partTime =
                    extractor.extract(partitionKeys, extractPartitionValues(new Path(partition)));
            if (watermark > toMills(partTime) + commitDelay) {
                needCommit.add(partition);
                iter.remove();
            }
        }
        return needCommit;
    }

    @Override
    public void snapshotState(long checkpointId, long watermark) throws Exception {
        pendingPartitionsState.clear();
        pendingPartitionsState.add(new ArrayList<>(pendingPartitions));

        watermarks.put(checkpointId, watermark);
        watermarksState.clear();
        watermarksState.add(new HashMap<>(watermarks));
    }

    @Override
    public List<String> endInput() {
        ArrayList<String> partitions = new ArrayList<>(pendingPartitions);
        pendingPartitions.clear();
        return partitions;
    }
}
