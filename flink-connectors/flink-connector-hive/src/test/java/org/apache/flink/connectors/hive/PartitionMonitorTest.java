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
import org.apache.flink.connectors.hive.read.HiveContinuousPartitionContext;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.filesystem.ContinuousPartitionFetcher;
import org.apache.flink.table.filesystem.PartitionFetcher;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.JobConf;
import org.apache.http.util.Asserts;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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

    private ContinuousHiveSplitEnumerator.PartitionMonitor<Long> partitionMonitor;
    private List<Partition> testPartitionWithOffset = new ArrayList<>();

    @Test
    public void testPartitionWithSameCreateTime() throws Exception {
        preparePartitionMonitor();
        commitPartitionWithGivenCreateTime(Arrays.asList("p1=A1", "p2=B1"), 1);
        commitPartitionWithGivenCreateTime(Arrays.asList("p1=A1", "p2=B2"), 2);
        ContinuousHiveSplitEnumerator.NewSplitsAndState<Long> newSplitsAndState =
                partitionMonitor.call();
        assertPartitionEquals(
                Arrays.asList(Arrays.asList("p1=A1", "p2=B1"), Arrays.asList("p1=A1", "p2=B2")),
                newSplitsAndState.getSeenPartitions());

        commitPartitionWithGivenCreateTime(Arrays.asList("p1=A1", "p2=B3"), 3);
        newSplitsAndState = partitionMonitor.call();
        assertPartitionEquals(
                Collections.singletonList(Arrays.asList("p1=A1", "p2=B3")),
                newSplitsAndState.getSeenPartitions());

        // check the partition with same create time can be monitored
        commitPartitionWithGivenCreateTime(Arrays.asList("p1=A1", "p2=B4"), 3);
        commitPartitionWithGivenCreateTime(Arrays.asList("p1=A1", "p2=B5"), 4);
        newSplitsAndState = partitionMonitor.call();
        assertPartitionEquals(
                Arrays.asList(Arrays.asList("p1=A1", "p2=B4"), Arrays.asList("p1=A1", "p2=B5")),
                newSplitsAndState.getSeenPartitions());
    }

    private void assertPartitionEquals(
            Collection<List<String>> expected, Collection<List<String>> actual) {
        assertTrue(expected != null && actual != null && expected.size() == actual.size());
        assertArrayEquals(
                expected.stream().map(Object::toString).sorted().toArray(),
                actual.stream().map(Object::toString).sorted().toArray());
    }

    private void commitPartitionWithGivenCreateTime(
            List<String> partitionValues, Integer createTime) {
        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation("/tmp/test");
        Partition partition =
                new Partition(
                        partitionValues, "testDb", "testTable", createTime, createTime, sd, null);

        partition.setValues(partitionValues);
        testPartitionWithOffset.add(partition);
    }

    private void preparePartitionMonitor() {
        List<List<String>> seenPartitionsSinceOffset = new ArrayList<>();
        JobConf jobConf = new JobConf();
        Configuration configuration = new Configuration();

        ObjectPath tablePath = new ObjectPath("testDb", "testTable");
        configuration.setString("streaming-source.consume-order", "create-time");

        HiveContinuousPartitionContext<Partition, Long> fetcherContext =
                new HiveContinuousPartitionContext<Partition, Long>() {
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
                    public ObjectPath getTablePath() {
                        return null;
                    }

                    @Override
                    public TypeSerializer<Long> getTypeSerializer() {
                        return null;
                    }

                    @Override
                    public Long getConsumeStartOffset() {
                        return null;
                    }

                    @Override
                    public void open() throws Exception {}

                    @Override
                    public Optional<Partition> getPartition(List<String> partValues)
                            throws Exception {
                        return Optional.empty();
                    }

                    @Override
                    public List<ComparablePartitionValue> getComparablePartitionValueList()
                            throws Exception {
                        return null;
                    }

                    @Override
                    public void close() throws Exception {}
                };

        ContinuousPartitionFetcher<Partition, Long> continuousPartitionFetcher =
                new ContinuousPartitionFetcher<Partition, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public List<Tuple2<Partition, Long>> fetchPartitions(
                            Context<Partition, Long> context, Long previousOffset)
                            throws Exception {
                        return testPartitionWithOffset.stream()
                                .filter(p -> (long) p.getCreateTime() >= previousOffset)
                                .map(p -> Tuple2.of(p, (long) p.getCreateTime()))
                                .collect(Collectors.toList());
                    }

                    @Override
                    public List<Partition> fetch(PartitionFetcher.Context<Partition> context)
                            throws Exception {
                        return null;
                    }
                };

        partitionMonitor =
                new ContinuousHiveSplitEnumerator.PartitionMonitor<>(
                        0L,
                        seenPartitionsSinceOffset,
                        tablePath,
                        jobConf,
                        continuousPartitionFetcher,
                        fetcherContext);
    }
}
