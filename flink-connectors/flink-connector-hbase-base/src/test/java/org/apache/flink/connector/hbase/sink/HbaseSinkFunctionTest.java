/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hbase.sink;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.junit.Test;

/** Test for {@link HBaseSinkFunction}. */
public class HbaseSinkFunctionTest {

    /**
     * To test whether the sink can throw exception when HBaseSinkFunction.numPendingRequests is 0
     * but HBaseSinkFunction.failureThrowable has exception.
     */
    @Test(expected = CheckpointException.class)
    public void testSnapshotStateWithError() throws Exception {
        HBaseTableSchema hBaseTableSchema = getHBaseTableSchema();

        HBaseSinkFunction sinkFunction =
                new HBaseSinkFunction(
                        "TEST-HBASE",
                        getHbaseConfig(),
                        new RowDataToMutationConverter(hBaseTableSchema, "null"),
                        1000,
                        1000,
                        1000);
        try (OneInputStreamOperatorTestHarness<RowData, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(
                        new StreamSink<>(sinkFunction), IntSerializer.INSTANCE)) {
            testHarness.setup();
            testHarness.open();
            testHarness.processElement(getTestRowData(), 0);
            // wait the scheduler thread call flush.
            Thread.sleep(5000);
            testHarness.snapshot(0, 1);
        }
    }

    private HBaseTableSchema getHBaseTableSchema() {
        HBaseTableSchema hBaseTableSchema = new HBaseTableSchema();
        hBaseTableSchema.setRowKey("rowkey", Integer.class);
        hBaseTableSchema.addColumn("TEST-FAMILY", "TEST-QUALIFIER", String.class);
        return hBaseTableSchema;
    }

    private Configuration getHbaseConfig() {
        Configuration config = new Configuration();
        config.set(HConstants.ZOOKEEPER_QUORUM, "localhost:2181/hbase");
        config.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "3000");
        config.set(HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY, "1000");
        config.set(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, "1000");
        config.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "0");
        return config;
    }

    public RowData getTestRowData() {
        GenericRowData outsideRow = new GenericRowData(2);
        outsideRow.setField(0, 1);
        GenericRowData insideRow = new GenericRowData(1);
        insideRow.setField(0, new BinaryStringData("TEST-VALUE"));
        outsideRow.setField(1, insideRow);
        return outsideRow;
    }
}
