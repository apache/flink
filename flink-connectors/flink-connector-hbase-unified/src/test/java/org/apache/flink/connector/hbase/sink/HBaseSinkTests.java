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

package org.apache.flink.connector.hbase.sink;

import org.apache.flink.connector.hbase.HBaseEvent;
import org.apache.flink.connector.hbase.testutil.HBaseTestCluster;
import org.apache.flink.connector.hbase.testutil.TestsWithTestHBaseCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link org.apache.flink.connector.hbase.sink.HBaseSink}. */
public class HBaseSinkTests extends TestsWithTestHBaseCluster {

    @Test
    public void testSinkPut() throws Exception {
        cluster.makeTable(baseTableName);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration hbaseConfiguration = cluster.getConfig();

        int start = 1;
        int end = 10;

        DataStream<Long> numberStream = env.fromSequence(start, end);

        final HBaseSink<Long> hbaseSink =
                HBaseSink.builder()
                        .setTableName(baseTableName)
                        .setSinkSerializer(new HBasePutLongSerializer())
                        .setHBaseConfiguration(hbaseConfiguration)
                        .build();
        numberStream.sinkTo(hbaseSink);
        env.execute();

        long[] expected = LongStream.rangeClosed(start, end).toArray();
        long[] actual = new long[end - start + 1];

        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration)) {
            Table table = connection.getTable(TableName.valueOf(baseTableName));
            for (int i = start; i <= end; i++) {
                Get get = new Get(Bytes.toBytes(String.valueOf(i)));
                Result r = table.get(get);
                byte[] value =
                        r.getValue(
                                HBaseTestCluster.DEFAULT_COLUMN_FAMILY.getBytes(),
                                HBaseTestCluster.DEFAULT_QUALIFIER.getBytes());
                long l = Long.parseLong(new String(value));
                actual[i - start] = l;
            }
        }

        assertArrayEquals(expected, actual);
    }

    @Test
    public void testSinkDelete() throws Exception {
        cluster.makeTable(baseTableName);
        List<String> rows = new ArrayList<>();
        String rowId;

        for (int i = 0; i < 10; i++) {
            rowId = cluster.put(baseTableName, String.valueOf(i));
            rows.add(rowId);
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration hbaseConfiguration = cluster.getConfig();

        DataStream<String> numberStream = env.fromCollection(rows);

        final HBaseSink<String> hbaseSink =
                HBaseSink.builder()
                        .setTableName(baseTableName)
                        .setSinkSerializer(new HBaseDeleteStringSerializer())
                        .setHBaseConfiguration(hbaseConfiguration)
                        .build();

        numberStream.sinkTo(hbaseSink);
        env.execute();

        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration)) {
            Table table = connection.getTable(TableName.valueOf(baseTableName));
            for (String row : rows) {
                Get get = new Get(Bytes.toBytes(row));
                Result r = table.get(get);
                assertTrue(r.isEmpty());
            }
        }
    }

    private static class HBasePutLongSerializer implements HBaseSinkSerializer<Long> {
        @Override
        public HBaseEvent serialize(Long event) {
            return HBaseEvent.putWith(
                    event.toString(),
                    HBaseTestCluster.DEFAULT_COLUMN_FAMILY,
                    HBaseTestCluster.DEFAULT_QUALIFIER,
                    Bytes.toBytes(event.toString()));
        }
    }

    private static class HBaseDeleteStringSerializer implements HBaseSinkSerializer<String> {
        @Override
        public HBaseEvent serialize(String event) {
            return HBaseEvent.deleteWith(
                    event,
                    HBaseTestCluster.DEFAULT_COLUMN_FAMILY,
                    HBaseTestCluster.DEFAULT_QUALIFIER);
        }
    }
}
