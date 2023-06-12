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

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.FileSystemTableSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_DELAY;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_KIND;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_TRIGGER;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link StreamingFileWriter}. */
class StreamingFileWriterTest {

    private final OutputFileConfig outputFileConfig = OutputFileConfig.builder().build();
    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @TempDir private java.nio.file.Path tmpDir;
    private Path path;

    @BeforeEach
    void before() throws IOException {
        path = new Path(tmpDir.resolve("tmp").toUri());
    }

    @Test
    void testFailover() throws Exception {
        OperatorSubtaskState state;
        try (OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> harness = create()) {
            harness.setup();
            harness.initializeEmptyState();
            harness.open();
            harness.processElement(row("1"), 0);
            harness.processElement(row("2"), 0);
            harness.processElement(row("2"), 0);
            state = harness.snapshot(1, 1);
            harness.processElement(row("3"), 0);
            harness.processElement(row("4"), 0);
            harness.notifyOfCompletedCheckpoint(1);
            List<String> partitions = collect(harness);
            assertThat(partitions).containsExactly("1", "2");
        }

        // first retry, no partition {1, 2} records
        try (OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> harness = create()) {
            harness.setup();
            harness.initializeState(state);
            harness.open();
            harness.processElement(row("3"), 0);
            harness.processElement(row("4"), 0);
            state = harness.snapshot(2, 2);
            harness.notifyOfCompletedCheckpoint(2);
            List<String> partitions = collect(harness);
            assertThat(partitions).containsExactly("1", "2", "3", "4");
        }

        // second retry, partition {4} repeat
        try (OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> harness = create()) {
            harness.setup();
            harness.initializeState(state);
            harness.open();
            harness.processElement(row("4"), 0);
            harness.processElement(row("5"), 0);
            state = harness.snapshot(3, 3);
            harness.notifyOfCompletedCheckpoint(3);
            List<String> partitions = collect(harness);
            assertThat(partitions).containsExactly("3", "4", "5");
        }

        // third retry, multiple snapshots
        try (OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> harness = create()) {
            harness.setup();
            harness.initializeState(state);
            harness.open();
            harness.processElement(row("6"), 0);
            harness.processElement(row("7"), 0);
            harness.snapshot(4, 4);
            harness.processElement(row("8"), 0);
            harness.snapshot(5, 5);
            harness.processElement(row("9"), 0);
            harness.snapshot(6, 6);
            harness.notifyOfCompletedCheckpoint(5);
            List<String> partitions = collect(harness);
            // should not contains partition {9}
            assertThat(partitions).containsExactly("4", "5", "6", "7", "8");
        }
    }

    @Test
    void testCommitImmediately() throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> harness = create()) {
            harness.setup();
            harness.initializeEmptyState();
            harness.open();

            harness.processElement(row("1"), 0);
            harness.processElement(row("2"), 0);
            harness.processElement(row("2"), 0);

            harness.snapshot(1, 1);

            // repeat partition 1
            harness.processElement(row("1"), 0);

            harness.processElement(row("3"), 0);
            harness.processElement(row("4"), 0);

            harness.notifyOfCompletedCheckpoint(1);
            List<String> partitions = collect(harness);
            assertThat(partitions).containsExactly("1", "2");
        }
    }

    @Test
    void testCommitFileWhenPartitionIsCommittableByProcessTime() throws Exception {
        // the rolling policy is not to roll file by filesize and roll file after one day,
        // it can ensure the file can be closed only when the partition is committable in this test.
        FileSystemTableSink.TableRollingPolicy tableRollingPolicy =
                new FileSystemTableSink.TableRollingPolicy(
                        false,
                        Long.MAX_VALUE,
                        Duration.ofDays(1).toMillis(),
                        Duration.ofDays(1).toMillis());
        List<String> partitionKeys = Collections.singletonList("d");
        // commit delay is 1 second with process-time trigger
        Configuration conf = getProcTimeCommitTriggerConf(Duration.ofSeconds(1).toMillis());
        OperatorSubtaskState state;
        long currentTimeMillis = System.currentTimeMillis();
        try (OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> harness =
                create(tableRollingPolicy, partitionKeys, conf)) {
            harness.setup();
            harness.initializeEmptyState();
            harness.open();
            harness.setProcessingTime(currentTimeMillis);
            harness.processElement(row("1"), 0);
            harness.processElement(row("2"), 0);
            state = harness.snapshot(1, 1);
            harness.processElement(row("3"), 0);
            harness.notifyOfCompletedCheckpoint(1);
            // assert files aren't committed in {1, 2} partitions
            assertThat(isPartitionFileCommitted("1", 0, 0)).isFalse();
            assertThat(isPartitionFileCommitted("2", 0, 1)).isFalse();
        }

        // first retry
        try (OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> harness =
                create(tableRollingPolicy, partitionKeys, conf)) {
            harness.setup();
            harness.initializeState(state);
            harness.open();
            harness.processElement(row("3"), 0);

            // simulate waiting for 2 seconds, now partition {3} is committable
            currentTimeMillis += Duration.ofSeconds(2).toMillis();
            harness.setProcessingTime(currentTimeMillis);
            harness.processElement(row("4"), 0);
            harness.snapshot(2, 2);
            harness.notifyOfCompletedCheckpoint(2);
            // only file in partition {3} should be committed
            // assert files are committed
            assertThat(isPartitionFileCommitted("3", 0, 2)).isTrue();
            assertThat(isPartitionFileCommitted("4", 0, 3)).isFalse();

            // simulate waiting for 2 seconds again, now partition {1} is committable
            currentTimeMillis += Duration.ofSeconds(2).toMillis();
            harness.setProcessingTime(currentTimeMillis);
            state = harness.snapshot(3, 3);
            harness.notifyOfCompletedCheckpoint(3);
            assertThat(isPartitionFileCommitted("4", 0, 3)).isTrue();
        }

        // second retry
        try (OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> harness =
                create(tableRollingPolicy, partitionKeys, conf)) {
            harness.setup();
            harness.initializeState(state);
            harness.open();

            harness.processElement(row("4"), 0);
            harness.processElement(row("4"), 0);
            harness.snapshot(4, 4);
            harness.processElement(row("5"), 5);
            harness.endInput();
            // assert files in all partition have been committed
            assertThat(isPartitionFileCommitted("4", 0, 4)).isTrue();
            assertThat(isPartitionFileCommitted("5", 0, 5)).isTrue();
        }
    }

    @Test
    void testCommitFileWhenPartitionIsCommittableByPartitionTime() throws Exception {
        // the rolling policy is not to roll file by filesize and roll file after one day,
        // it can ensure the file can be closed only when the partition is committable in this test.
        FileSystemTableSink.TableRollingPolicy tableRollingPolicy =
                new FileSystemTableSink.TableRollingPolicy(
                        false,
                        Long.MAX_VALUE,
                        Duration.ofDays(1).toMillis(),
                        Duration.ofDays(1).toMillis());
        List<String> partitionKeys = Collections.singletonList("d");
        // commit delay is 1 day with partition-time trigger
        Configuration conf = getPartitionCommitTriggerConf(Duration.ofDays(1).toMillis());

        long currentTimeMillis = System.currentTimeMillis();

        Date nextYear = new Date(currentTimeMillis + Duration.ofDays(365).toMillis());
        String nextYearPartition = "d=" + dateFormat.format(nextYear);
        Date yesterday = new Date(currentTimeMillis - Duration.ofDays(1).toMillis());
        String yesterdayPartition = "d=" + dateFormat.format(yesterday);
        Date today = new Date(currentTimeMillis);
        String todayPartition = "d=" + dateFormat.format(today);
        Date tomorrow = new Date(currentTimeMillis + Duration.ofDays(1).toMillis());
        String tomorrowPartition = "d=" + dateFormat.format(tomorrow);

        OperatorSubtaskState state;
        try (OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> harness =
                create(tableRollingPolicy, partitionKeys, conf)) {
            harness.setup();
            harness.initializeEmptyState();
            harness.open();
            harness.processElement(row(yesterdayPartition), 0);
            harness.processWatermark(currentTimeMillis);
            state = harness.snapshot(1, 1);
            harness.notifyOfCompletedCheckpoint(1);
            // assert yesterday partition file is committed
            assertThat(isPartitionFileCommitted(yesterdayPartition, 0, 0)).isTrue();
        }

        // first retry
        try (OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> harness =
                create(tableRollingPolicy, partitionKeys, conf)) {
            harness.setup();
            harness.initializeState(state);
            harness.open();
            harness.processElement(row(tomorrowPartition), 0);
            harness.processElement(row(todayPartition), 0);

            // simulate waiting for 1 day
            currentTimeMillis += Duration.ofDays(1).toMillis();
            harness.processWatermark(currentTimeMillis);
            harness.snapshot(2, 2);
            harness.notifyOfCompletedCheckpoint(2);
            // assert today partition file is committed
            assertThat(isPartitionFileCommitted(todayPartition, 0, 2)).isTrue();
            // assert tomorrow partition file isn't committed
            assertThat(isPartitionFileCommitted(tomorrowPartition, 0, 1)).isFalse();

            // simulate waiting for 1 day again, now tomorrow partition is committable
            currentTimeMillis += Duration.ofDays(1).toMillis();
            harness.processWatermark(currentTimeMillis);
            state = harness.snapshot(3, 3);
            harness.notifyOfCompletedCheckpoint(3);
            assertThat(isPartitionFileCommitted(tomorrowPartition, 0, 1)).isTrue();

            harness.processElement(row(nextYearPartition), 0);
        }

        // second retry
        try (OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> harness =
                create(tableRollingPolicy, partitionKeys, conf)) {
            harness.setup();
            harness.initializeState(state);
            harness.open();

            harness.processElement(row(nextYearPartition), 0);
            harness.processElement(row(tomorrowPartition), 0);
            harness.endInput();
            // assert files in all partition have been committed
            assertThat(isPartitionFileCommitted(tomorrowPartition, 0, 4)).isTrue();
            assertThat(isPartitionFileCommitted(nextYearPartition, 0, 3)).isTrue();
        }
    }

    private static RowData row(String s) {
        return GenericRowData.of(StringData.fromString(s));
    }

    private static List<String> collect(
            OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> harness) {
        List<String> parts = new ArrayList<>();
        harness.extractOutputValues().forEach(m -> parts.addAll(Arrays.asList(m.getPartitions())));
        return parts;
    }

    private OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> create()
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(SINK_PARTITION_COMMIT_TRIGGER.key(), "process-time");
        return create(OnCheckpointRollingPolicy.build(), new ArrayList<>(), configuration);
    }

    private OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> create(
            RollingPolicy<RowData, String> rollingPolicy,
            List<String> partitionKeys,
            Configuration conf)
            throws Exception {
        StreamingFileWriter<RowData> writer =
                new StreamingFileWriter<>(
                        1000,
                        StreamingFileSink.forRowFormat(
                                        path,
                                        (Encoder<RowData>)
                                                (element, stream) ->
                                                        stream.write(
                                                                (element.getString(0) + "\n")
                                                                        .getBytes(
                                                                                StandardCharsets
                                                                                        .UTF_8)))
                                .withBucketAssigner(
                                        new BucketAssigner<RowData, String>() {
                                            @Override
                                            public String getBucketId(
                                                    RowData element, Context context) {
                                                return element.getString(0).toString();
                                            }

                                            @Override
                                            public SimpleVersionedSerializer<String>
                                                    getSerializer() {
                                                return SimpleVersionedStringSerializer.INSTANCE;
                                            }
                                        })
                                .withRollingPolicy(rollingPolicy),
                        partitionKeys,
                        conf);
        OneInputStreamOperatorTestHarness<RowData, PartitionCommitInfo> harness =
                new OneInputStreamOperatorTestHarness<>(writer, 1, 1, 0);
        harness.getStreamConfig().setTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        return harness;
    }

    private Configuration getPartitionCommitTriggerConf(long commitDelay) {
        Configuration configuration = new Configuration();
        configuration.setString(SINK_PARTITION_COMMIT_POLICY_KIND, "success-file");
        configuration.setString(PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER.key(), "yyyy-MM-dd");
        configuration.setString(SINK_PARTITION_COMMIT_TRIGGER.key(), "partition-time");
        configuration.setLong(SINK_PARTITION_COMMIT_DELAY.key(), commitDelay);
        configuration.setString(SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE.key(), "UTC");
        return configuration;
    }

    private Configuration getProcTimeCommitTriggerConf(long commitDelay) {
        Configuration configuration = new Configuration();
        configuration.setString(SINK_PARTITION_COMMIT_POLICY_KIND, "success-file");
        configuration.setString(SINK_PARTITION_COMMIT_TRIGGER.key(), "process-time");
        configuration.setLong(SINK_PARTITION_COMMIT_DELAY.key(), commitDelay);
        configuration.setString(SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE.key(), "UTC");
        return configuration;
    }

    private boolean isPartitionFileCommitted(String partition, int subtaskIndex, int partCounter) {
        java.nio.file.Path bucketPath = Paths.get(path.getPath(), partition);
        String fileName =
                outputFileConfig.getPartPrefix()
                        + '-'
                        + subtaskIndex
                        + '-'
                        + partCounter
                        + outputFileConfig.getPartSuffix();
        java.nio.file.Path filePath = bucketPath.resolve(fileName);
        return filePath.toFile().exists();
    }
}
