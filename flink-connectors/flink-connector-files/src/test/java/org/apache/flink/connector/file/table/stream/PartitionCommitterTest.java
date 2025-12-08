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

package org.apache.flink.connector.file.table.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.FileSystemCommitterTest;
import org.apache.flink.connector.file.table.FileSystemFactory;
import org.apache.flink.connector.file.table.TableMetaStoreFactory;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_DELAY;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_KIND;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_TRIGGER;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE;

class PartitionCommitterTest {

    @TempDir private java.nio.file.Path tmpDir;
    private Path path;
    private final FileSystemFactory fileSystemFactory = FileSystem::get;
    private TableMetaStoreFactory metaStoreFactory;

    @BeforeEach
    void before() throws IOException {
        path = new Path(tmpDir.resolve("tmp").toUri());
        metaStoreFactory =
                new FileSystemCommitterTest.TestMetaStoreFactory(new Path(path.toString()));
    }

    @Test
    void testSwitchTriggerFromPartitionToProc() throws Exception {
        List<String> partitionKeys = List.of("dt");
        OperatorSubtaskState state;
        Configuration partitionCommitTriggerConf =
                getPartitionCommitTriggerConf(Duration.ofSeconds(1).toMillis());
        try (OneInputStreamOperatorTestHarness<PartitionCommitInfo, Void> harness =
                create(partitionKeys, partitionCommitTriggerConf)) {
            harness.setup();
            harness.initializeEmptyState();
            harness.open();
            state = harness.snapshot(1, 1);
        }
        Configuration procTimeCommitTriggerConf =
                getProcTimeCommitTriggerConf(Duration.ofSeconds(1).toMillis());
        try (OneInputStreamOperatorTestHarness<PartitionCommitInfo, Void> harness =
                create(partitionKeys, procTimeCommitTriggerConf)) {
            harness.setup();
            harness.setRestoredCheckpointId(1L);
            Assertions.assertDoesNotThrow(() -> harness.initializeState(state));
        }
    }

    @Test
    void testSwitchTriggerFromProcToPartition() throws Exception {
        List<String> partitionKeys = List.of("dt");
        OperatorSubtaskState state;
        Configuration procTimeCommitTriggerConf =
                getProcTimeCommitTriggerConf(Duration.ofSeconds(1).toMillis());
        try (OneInputStreamOperatorTestHarness<PartitionCommitInfo, Void> harness =
                create(partitionKeys, procTimeCommitTriggerConf)) {
            harness.setup();
            harness.setRestoredCheckpointId(1L);
            harness.initializeEmptyState();
            harness.open();
            state = harness.snapshot(1, 1);
        }
        Configuration partitionCommitTriggerConf =
                getPartitionCommitTriggerConf(Duration.ofSeconds(1).toMillis());
        try (OneInputStreamOperatorTestHarness<PartitionCommitInfo, Void> harness =
                create(partitionKeys, partitionCommitTriggerConf)) {
            harness.setup();
            Assertions.assertDoesNotThrow(() -> harness.initializeState(state));
        }
    }

    private Configuration getPartitionCommitTriggerConf(long commitDelay) {
        Configuration configuration = new Configuration();
        configuration.set(SINK_PARTITION_COMMIT_POLICY_KIND, "success-file");
        configuration.setString(PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER.key(), "yyyy-MM-dd");
        configuration.setString(SINK_PARTITION_COMMIT_TRIGGER.key(), "partition-time");
        configuration.set(SINK_PARTITION_COMMIT_DELAY, Duration.ofMillis(commitDelay));
        configuration.setString(SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE.key(), "UTC");
        return configuration;
    }

    private Configuration getProcTimeCommitTriggerConf(long commitDelay) {
        Configuration configuration = new Configuration();
        configuration.set(SINK_PARTITION_COMMIT_POLICY_KIND, "success-file");
        configuration.setString(SINK_PARTITION_COMMIT_TRIGGER.key(), "process-time");
        configuration.set(SINK_PARTITION_COMMIT_DELAY, Duration.ofMillis(commitDelay));
        configuration.setString(SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE.key(), "UTC");
        return configuration;
    }

    private OneInputStreamOperatorTestHarness<PartitionCommitInfo, Void> create(
            List<String> partitionKeys, Configuration conf) throws Exception {
        PartitionCommitter committer =
                new PartitionCommitter(path, null, partitionKeys, null, fileSystemFactory, conf);
        return new OneInputStreamOperatorTestHarness<>(committer, 1, 1, 0);
    }
}
