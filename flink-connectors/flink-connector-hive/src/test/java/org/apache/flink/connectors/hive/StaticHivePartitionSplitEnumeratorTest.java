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

import org.apache.flink.connector.file.src.assigners.SimpleSplitAssigner;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.core.fs.Path;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link StaticHivePartitionSplitEnumerator}. */
class StaticHivePartitionSplitEnumeratorTest {

    private static final int PARALLELISM = 10;
    private TemporaryFolder temporaryFolder;

    @BeforeEach
    void setup() throws IOException {
        temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
    }

    @Test
    void testDiscoverSplitWhenNoReaderRegistered() throws Exception {
        final TestingSplitEnumeratorContext<HiveSourceSplit> context =
                new TestingSplitEnumeratorContext<>(PARALLELISM);
        final HiveSourceSplit split = createSplit();
        final StaticHivePartitionSplitEnumerator<Long> enumerator =
                new StaticHivePartitionSplitEnumerator<>(
                        context,
                        new ArrayDeque<>(),
                        new SimpleSplitAssigner(Collections.singletonList(split)),
                        new JobConf());
        enumerator.start();
        assertThat(enumerator.snapshotState(1L).getSplits()).contains(split);
    }

    @Test
    public void testRequestSplitWhenReaderRegistered() throws Exception {
        TestingSplitEnumeratorContext<HiveSourceSplit> enumeratorContext =
                new TestingSplitEnumeratorContext<>(PARALLELISM);
        List<HiveTablePartition> partitions = testCreateHiveTablePartitions(PARALLELISM);
        JobConf jobConf = new JobConf();
        jobConf.set(HiveOptions.TABLE_EXEC_HIVE_CALCULATE_PARTITION_SIZE_THREAD_NUM.key(), "1");
        final StaticHivePartitionSplitEnumerator<Long> enumerator =
                new StaticHivePartitionSplitEnumerator<>(
                        enumeratorContext,
                        new ArrayDeque<>(partitions),
                        new SimpleSplitAssigner(Collections.emptyList()),
                        jobConf);
        // register multi readers, and let them request a split
        enumeratorContext.registerReader(1, "localhost");
        assertThat(enumeratorContext.registeredReaders()).hasSize(1);
        for (int num = 0; num < PARALLELISM; ++num) {
            enumerator.handleSplitRequest(1, "localhost");
        }
        assertThat(enumeratorContext.getSplitAssignments().get(1).getAssignedSplits())
                .hasSize(PARALLELISM);
        enumerator.handleSplitRequest(1, "localhost");
        assertThat(enumeratorContext.getSplitAssignments().get(1).hasReceivedNoMoreSplitsSignal())
                .isTrue();
    }

    @Test
    public void testAddSplitsBack() throws Exception {
        final TestingSplitEnumeratorContext<HiveSourceSplit> context =
                new TestingSplitEnumeratorContext<>(PARALLELISM);
        final StaticHivePartitionSplitEnumerator<Long> enumerator =
                new StaticHivePartitionSplitEnumerator<>(
                        context,
                        new ArrayDeque<>(),
                        new SimpleSplitAssigner(Collections.emptyList()),
                        new JobConf());
        final HiveSourceSplit split = createSplit();
        enumerator.addSplitsBack(Collections.singletonList(split), 0);
        assertThat(enumerator.snapshotState(0).getSplits()).contains(split);
        assertThat(enumerator.snapshotState(0).getAlreadyProcessedPaths()).isEmpty();
    }

    private HiveSourceSplit createSplit() {
        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation("/tmp");
        return new HiveSourceSplit(
                "1",
                new Path("/tmp"),
                0,
                0,
                0,
                0,
                new String[] {"host1"},
                null,
                new HiveTablePartition(sd, new HashMap<>(), new Properties()));
    }

    public List<HiveTablePartition> testCreateHiveTablePartitions(int partitionNumber)
            throws Exception {
        List<HiveTablePartition> partitions = new ArrayList<>();
        StorageDescriptor sd = new StorageDescriptor();
        // set orc format
        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setSerializationLib(OrcSerde.class.getName());
        File wareHouse = temporaryFolder.newFolder("testCreateInputSplits");
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(OrcInputFormat.class.getName());
        sd.setLocation(wareHouse.toString());
        String baseFilePath =
                Objects.requireNonNull(this.getClass().getResource("/orc/test.orc")).getPath();
        Files.copy(Paths.get(baseFilePath), Paths.get(wareHouse.toString(), "t.orc"));
        for (int num = 0; num < partitionNumber; ++num) {
            partitions.add(new HiveTablePartition(sd, new Properties()));
        }
        return partitions;
    }
}
