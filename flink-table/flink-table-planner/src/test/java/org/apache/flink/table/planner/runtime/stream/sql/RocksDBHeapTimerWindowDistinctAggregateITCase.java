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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests split distinct window aggregation with RocksDB state and heap timers. */
class RocksDBHeapTimerWindowDistinctAggregateITCase extends StreamingWithStateTestBase {

    RocksDBHeapTimerWindowDistinctAggregateITCase() {
        super(StreamingWithStateTestBase.ROCKSDB_BACKEND());
    }

    @BeforeEach
    @Override
    public void before() {
        super.before();

        final EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend();
        stateBackend.setPriorityQueueStateType(
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP);
        env().setStateBackend(stateBackend);

        final String dataId = TestValuesTableFactory.registerData(testData());
        tEnv().executeSql(
                        "CREATE TABLE T1 (\n"
                                + " `id` BIGINT,\n"
                                + " `ts` STRING,\n"
                                + " `card_no_id` AS MOD(`id`, 1000),\n"
                                + " `rowtime` AS TO_TIMESTAMP(`ts`),\n"
                                + " WATERMARK FOR `rowtime` AS `rowtime` - INTERVAL '1' SECOND\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '"
                                + dataId
                                + "'\n"
                                + ")");
        tEnv().getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true);
    }

    @Test
    void testCumulateWindowDistinctCountDoesNotLoseHeapTimerKeys() throws Exception {
        tEnv().executeSql(
                        "CREATE TABLE sink (\n"
                                + " window_start TIMESTAMP(3),\n"
                                + " window_end TIMESTAMP(3),\n"
                                + " distinct_cnt BIGINT\n"
                                + ") WITH ('connector' = 'values')");

        tEnv().executeSql(
                        "INSERT INTO sink\n"
                                + "SELECT window_start, window_end, COUNT(DISTINCT `card_no_id`)\n"
                                + "FROM TABLE(\n"
                                + " CUMULATE(\n"
                                + "  TABLE T1,\n"
                                + "  DESCRIPTOR(rowtime),\n"
                                + "  INTERVAL '5' SECOND,\n"
                                + "  INTERVAL '20' SECOND))\n"
                                + "GROUP BY window_start, window_end")
                .await();

        assertThat(TestValuesTableFactory.getResultsAsStrings("sink"))
                .containsExactlyInAnyOrder(
                        "+I[2020-10-10T00:00, 2020-10-10T00:00:05, 350]",
                        "+I[2020-10-10T00:00, 2020-10-10T00:00:10, 700]",
                        "+I[2020-10-10T00:00, 2020-10-10T00:00:15, 1000]",
                        "+I[2020-10-10T00:00, 2020-10-10T00:00:20, 1000]");
    }

    private static List<Row> testData() {
        final int recordsPerSecond = 70;
        final int windowSizeSeconds = 20;
        final LocalDateTime start = LocalDateTime.of(2020, 10, 10, 0, 0);
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        final List<Row> rows = new ArrayList<>(recordsPerSecond * windowSizeSeconds);

        for (int id = 0; id < recordsPerSecond * windowSizeSeconds; id++) {
            rows.add(Row.of((long) id, start.plusSeconds(id / recordsPerSecond).format(formatter)));
        }
        return rows;
    }
}
