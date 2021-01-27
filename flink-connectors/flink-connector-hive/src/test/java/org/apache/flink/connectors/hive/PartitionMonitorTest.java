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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.filesystem.ContinuousPartitionFetcher;
import org.apache.flink.table.filesystem.PartitionFetcher;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.JobConf;
import org.apache.http.util.Asserts;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link ContinuousHiveSplitEnumerator.PartitionMonitor}. */
public class PartitionMonitorTest {

    private ContinuousHiveSplitEnumerator.PartitionMonitor partitionMonitor;
    private List<Partition> testPartitionWithOffset = new ArrayList<>();

    @Test
    public void testPartitionWithSameCreateTime() throws Exception {
        preparePartitionMonitor(0L);
        commitPartitionWithGivenCreateTime(Arrays.asList("p1=A1", "p2=B1"), 1);
        commitPartitionWithGivenCreateTime(Arrays.asList("p1=A1", "p2=B2"), 2);
        ContinuousHiveSplitEnumerator.NewSplitsAndState<Long> newSplitsAndState =
                partitionMonitor.call();
        assertPartitionEquals(
                Arrays.asList(Arrays.asList("p1=A1", "p2=B1"), Arrays.asList("p1=A1", "p2=B2")),
                newSplitsAndState.seenPartitions);

        commitPartitionWithGivenCreateTime(Arrays.asList("p1=A1", "p2=B3"), 3);
        newSplitsAndState = partitionMonitor.call();
        assertPartitionEquals(
                Arrays.asList(Arrays.asList("p1=A1", "p2=B3")), newSplitsAndState.seenPartitions);

        // check the partition with same create time can be monitored
        commitPartitionWithGivenCreateTime(Arrays.asList("p1=A1", "p2=B4"), 3);
        commitPartitionWithGivenCreateTime(Arrays.asList("p1=A1", "p2=B5"), 4);
        newSplitsAndState = partitionMonitor.call();
        assertPartitionEquals(
                Arrays.asList(Arrays.asList("p1=A1", "p2=B4"), Arrays.asList("p1=A1", "p2=B5")),
                newSplitsAndState.seenPartitions);
    }

    private void assertPartitionEquals(
            Collection<List<String>> expected, Collection<List<String>> actual) {
        assertTrue(expected != null && actual != null && expected.size() == actual.size());
        assertArrayEquals(
                expected.stream()
                        .map(p -> p.toString())
                        .sorted()
                        .collect(Collectors.toList())
                        .toArray(),
                actual.stream()
                        .map(p -> p.toString())
                        .sorted()
                        .collect(Collectors.toList())
                        .toArray());
    }

    private void commitPartitionWithGivenCreateTime(
            List<String> partitionValues, Integer createTime) {
        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation("/test");
        Partition partition =
                new Partition(
                        partitionValues, "testDb", "testTable", createTime, createTime, sd, null);

        partition.setValues(partitionValues);
        testPartitionWithOffset.add(partition);
    }

    private void preparePartitionMonitor(Long currentReadOffset) {
        List<List<String>> seenPartitionsSinceOffset = new ArrayList<>();
        JobConf jobConf = new JobConf();
        Configuration configuration = new Configuration();

        ObjectPath tablePath = new ObjectPath("testDb", "testTable");
        configuration.setString("streaming-source.consume-order", "create-time");
        HiveTableSource.HiveContinuousPartitionFetcherContext<Long> fetcherContext =
                new HiveTableSource.HiveContinuousPartitionFetcherContext<Long>(
                        null, null, null, null, null, null, configuration, null) {

                    @Override
                    public Optional<Partition> getPartition(List<String> partValues)
                            throws TException {
                        return super.getPartition(partValues);
                    }

                    @Override
                    public ObjectPath getTablePath() {
                        return super.getTablePath();
                    }

                    @Override
                    public long getModificationTime(Partition partition, Long partitionOffset) {
                        return super.getModificationTime(partition, partitionOffset);
                    }

                    @Override
                    public HiveTablePartition toHiveTablePartition(Partition partition) {
                        StorageDescriptor sd = partition.getSd();
                        Map<String, Object> partitionColValues = new HashMap<>();
                        for (String partCol : partition.getValues()) {
                            String[] arr = partCol.split("=");
                            Asserts.check(
                                    arr.length == 2, "partition string should be key=value format");
                            partitionColValues.put(arr[0], arr[1]);
                        }
                        return new HiveTablePartition(sd, partitionColValues, new Properties());
                    }

                    @Override
                    public TypeSerializer<Long> getTypeSerializer() {
                        return super.getTypeSerializer();
                    }

                    @Override
                    public Long getConsumeStartOffset() {
                        return super.getConsumeStartOffset();
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }

                    @Override
                    public void open() throws Exception {
                        super.open();
                    }

                    @Override
                    public List<ComparablePartitionValue> getComparablePartitionValueList()
                            throws Exception {
                        return super.getComparablePartitionValueList();
                    }
                };

        ContinuousPartitionFetcher<Partition, Long> continuousPartitionFetcher =
                new ContinuousPartitionFetcher<Partition, Long>() {

                    @Override
                    public List<Tuple2<Partition, Long>> fetchPartitions(
                            Context<Partition, Long> context, Long previousOffset)
                            throws Exception {
                        return testPartitionWithOffset.stream()
                                .filter(p -> Long.valueOf(p.getCreateTime()) >= previousOffset)
                                .map(p -> Tuple2.of(p, Long.valueOf(p.getCreateTime())))
                                .collect(Collectors.toList());
                    }

                    @Override
                    public List<Partition> fetch(PartitionFetcher.Context<Partition> context)
                            throws Exception {
                        return null;
                    }
                };

        partitionMonitor =
                new ContinuousHiveSplitEnumerator.PartitionMonitor(
                        currentReadOffset,
                        seenPartitionsSinceOffset,
                        tablePath,
                        jobConf,
                        continuousPartitionFetcher,
                        fetcherContext);
    }
}
