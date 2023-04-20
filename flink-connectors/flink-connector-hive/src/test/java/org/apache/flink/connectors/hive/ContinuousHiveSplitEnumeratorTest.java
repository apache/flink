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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.assigners.SimpleSplitAssigner;
import org.apache.flink.connector.file.table.ContinuousPartitionFetcher;
import org.apache.flink.connector.file.table.PartitionFetcher;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectPath;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link ContinuousHiveSplitEnumerator}. */
public class ContinuousHiveSplitEnumeratorTest {

    @Test
    public void testDiscoverSplitWhenNoReaderRegistered() throws Exception {
        final TestingSplitEnumeratorContext<HiveSourceSplit> context =
                new TestingSplitEnumeratorContext<>(4);
        final HiveSourceSplit split = createSplit();
        final ContinuousHiveSplitEnumerator<Long> enumerator =
                new ContinuousHiveSplitEnumerator(
                        context,
                        0L,
                        Collections.emptySet(),
                        new SimpleSplitAssigner(Collections.singletonList(split)),
                        1000L,
                        new JobConf(),
                        new ObjectPath("testDb", "testTable"),
                        mockPartitionFetcher(),
                        new MockHiveContinuousPartitionFetcherContext(
                                new ObjectPath("testDb", "testTable")));
        enumerator.start();
        context.triggerAllActions();

        assertThat(enumerator.snapshotState(1L).getSplits()).contains(split);
    }

    @Test
    public void testDiscoverWhenReaderRegistered() throws Exception {
        final TestingSplitEnumeratorContext<HiveSourceSplit> context =
                new TestingSplitEnumeratorContext<>(4);

        final ContinuousHiveSplitEnumerator<Long> enumerator =
                new ContinuousHiveSplitEnumerator(
                        context,
                        0L,
                        Collections.emptySet(),
                        new SimpleSplitAssigner(Collections.emptyList()),
                        1000L,
                        new JobConf(),
                        new ObjectPath("testDb", "testTable"),
                        mockPartitionFetcher(),
                        new MockHiveContinuousPartitionFetcherContext(
                                new ObjectPath("testDb", "testTable")));
        enumerator.start();
        // register one reader, and let it request a split
        context.registerReader(2, "localhost");
        enumerator.addReader(2);
        enumerator.handleSplitRequest(2, "localhost");
        final HiveSourceSplit split = createSplit();
        enumerator.addSplitsBack(Collections.singletonList(split), 0);
        context.triggerAllActions();

        assertThat(enumerator.snapshotState(1L).getSplits()).isEmpty();
        assertThat(context.getSplitAssignments().get(2).getAssignedSplits()).contains(split);
    }

    @Test
    public void testRequestingReaderUnavailableWhenSplitDiscovered() throws Exception {
        final TestingSplitEnumeratorContext<HiveSourceSplit> context =
                new TestingSplitEnumeratorContext<>(4);

        final ContinuousHiveSplitEnumerator<Long> enumerator =
                new ContinuousHiveSplitEnumerator(
                        context,
                        0L,
                        Collections.emptySet(),
                        new SimpleSplitAssigner(Collections.emptyList()),
                        1000L,
                        new JobConf(),
                        new ObjectPath("testDb", "testTable"),
                        mockPartitionFetcher(),
                        new MockHiveContinuousPartitionFetcherContext(
                                new ObjectPath("testDb", "testTable")));
        enumerator.start();
        // register one reader, and let it request a split
        context.registerReader(2, "localhost");
        enumerator.addReader(2);
        enumerator.handleSplitRequest(2, "localhost");

        // remove the reader (like in a failure)
        context.registeredReaders().remove(2);
        final HiveSourceSplit split = createSplit();
        enumerator.addSplitsBack(Collections.singletonList(split), 0);
        context.triggerAllActions();

        assertThat(context.getSplitAssignments()).doesNotContainKey(2);
        assertThat(enumerator.snapshotState(1L).getSplits()).contains(split);
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

    private ContinuousPartitionFetcher<Partition, Long> mockPartitionFetcher() {
        return new ContinuousPartitionFetcher<Partition, Long>() {

            private static final long serialVersionUID = 1L;

            @Override
            public List<Tuple2<Partition, Long>> fetchPartitions(
                    Context<Partition, Long> context, Long previousOffset) throws Exception {
                return Collections.emptyList();
            }

            @Override
            public List<Partition> fetch(PartitionFetcher.Context<Partition> context)
                    throws Exception {
                return Collections.emptyList();
            }
        };
    }

    private static class MockHiveContinuousPartitionFetcherContext
            extends HiveTableSource.HiveContinuousPartitionFetcherContext<Long> {

        public MockHiveContinuousPartitionFetcherContext(ObjectPath tablePath) {
            super(tablePath, null, null, null, new Configuration(), "default");
        }

        @Override
        public void close() throws Exception {}

        @Override
        public void open() throws Exception {}

        @Override
        public Optional<Partition> getPartition(List<String> partValues) throws TException {
            return Optional.empty();
        }

        @Override
        public HiveTablePartition toHiveTablePartition(Partition partition) {
            return new HiveTablePartition(partition.getSd(), new HashMap<>(), new Properties());
        }
    }
}
