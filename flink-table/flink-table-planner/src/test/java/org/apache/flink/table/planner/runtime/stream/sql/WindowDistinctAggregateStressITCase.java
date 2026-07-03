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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.FailingCollectionSource;
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stress test for FLINK-39481: with unaligned checkpoints and interruptible timers, a watermark
 * whose timer-firing chain was interrupted must still be forwarded downstream before EndOfData,
 * otherwise windows that only fire on that watermark lose their state. The deterministic coverage
 * lives in {@code UnalignedCheckpointsInterruptibleTimersTest}; this test additionally exercises
 * the full SQL pipeline (two-phase split-distinct window aggregation on RocksDB) under
 * backpressure, where the interruption races end of input, and compares against a golden run of the
 * identical topology.
 */
class WindowDistinctAggregateStressITCase extends StreamingWithStateTestBase {

    private static final Duration TEST_DURATION = Duration.ofSeconds(20);
    private static final long SINK_OPEN_DELAY_MS = 300;
    private static final long SINK_ROW_DELAY_MS = 1;

    private static final int ROWS_PER_SECOND = 20;
    private static final int LAST_SECOND = 34;
    private static final int NUM_KEYS = 200;
    private static final int STRING_CARDINALITY = 17;

    private int sinkCount;

    WindowDistinctAggregateStressITCase() {
        super(StreamingWithStateTestBase.ROCKSDB_BACKEND());
    }

    @BeforeEach
    @Override
    public void before() {
        super.before();
        env().enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE);
        final Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixeddelay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(0));
        // the configuration under which every FLINK-39481 CI failure occurred: every checkpoint
        // is unaligned and may interrupt firing timers; also set on the table config, which
        // builds the job configuration for table programs
        configuration.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        configuration.set(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT, Duration.ZERO);
        configuration.set(CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS, true);
        configuration.set(CheckpointingOptions.FILE_MERGING_ENABLED, true);
        configuration.set(ExecutionOptions.SNAPSHOT_COMPRESSION, true);
        configuration.setString("state.backend.rocksdb.use-ingest-db-restore-mode", "true");
        env().configure(configuration, Thread.currentThread().getContextClassLoader());
        configuration.toMap().forEach((k, v) -> tEnv().getConfig().set(k, v));

        final String dataId = TestValuesTableFactory.registerData(generatedRows());
        tEnv().executeSql(
                        "CREATE TABLE T1 (\n"
                                + " `ts` STRING,\n"
                                + " `int` INT,\n"
                                + " `double` DOUBLE,\n"
                                + " `float` FLOAT,\n"
                                + " `bigdec` DECIMAL(10, 2),\n"
                                + " `string` STRING,\n"
                                + " `name` STRING,\n"
                                + " `rowtime` AS TO_TIMESTAMP(`ts`),\n"
                                + " WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '"
                                + dataId
                                + "',\n"
                                + " 'failing-source' = 'true'\n"
                                + ")");

        tEnv().getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true);
        tEnv().createTemporarySystemFunction("throttle", ThrottlingFunction.class);
    }

    /** Many keys per 5s slice, so that each slice boundary fires a long chain of timers. */
    private static List<Row> generatedRows() {
        final List<Row> rows = new ArrayList<>();
        for (int sec = 0; sec <= LAST_SECOND; sec++) {
            for (int i = 0; i < ROWS_PER_SECOND; i++) {
                final int idx = sec * ROWS_PER_SECOND + i;
                rows.add(
                        Row.of(
                                String.format("2020-10-10 00:00:%02d", sec),
                                idx % 10,
                                (double) (idx % 7),
                                (float) (idx % 5),
                                new BigDecimal(String.format("%d.%02d", idx % 9, idx % 100)),
                                idx % 19 == 0 ? null : "s" + (idx % STRING_CARDINALITY),
                                idx % 23 == 0 ? null : String.format("k%03d", idx % NUM_KEYS)));
            }
        }
        return rows;
    }

    @Test
    void testCumulateWindowRollupUnderBackpressure() throws Exception {
        // golden run: identical topology, but the source does not fail (failedBefore is true)
        final List<String> expected = runQuery();
        assertThat(expected).isNotEmpty();

        final Deadline deadline = Deadline.fromNow(TEST_DURATION);
        do {
            // each round: checkpoint under backpressure, artificial failure, restore, end of
            // input
            FailingCollectionSource.reset();
            final List<String> actual = runQuery();
            assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        } while (deadline.hasTimeLeft());
    }

    private List<String> runQuery() throws Exception {
        final String sinkName = "sink_" + sinkCount++;
        tEnv().executeSql(
                        "CREATE TABLE "
                                + sinkName
                                + " (\n"
                                + " g BIGINT,\n"
                                + " name STRING,\n"
                                + " window_start TIMESTAMP(3),\n"
                                + " window_end TIMESTAMP(3),\n"
                                + " cnt BIGINT,\n"
                                + " sum_dec DECIMAL(38, 2),\n"
                                + " max_d DOUBLE,\n"
                                + " min_f FLOAT,\n"
                                + " distinct_cnt BIGINT\n"
                                + ") WITH ('connector' = 'values')");
        tEnv().executeSql(
                        "INSERT INTO "
                                + sinkName
                                + "\n"
                                + "SELECT\n"
                                + "  throttle(GROUPING_ID(`name`)),\n"
                                + "  `name`,\n"
                                + "  window_start,\n"
                                + "  window_end,\n"
                                + "  COUNT(*),\n"
                                + "  SUM(`bigdec`),\n"
                                + "  MAX(`double`),\n"
                                + "  MIN(`float`),\n"
                                + "  COUNT(DISTINCT `string`)\n"
                                + "FROM TABLE(\n"
                                + "   CUMULATE(\n"
                                + "     TABLE T1,\n"
                                + "     DESCRIPTOR(rowtime),\n"
                                + "     INTERVAL '5' SECOND,\n"
                                + "     INTERVAL '15' SECOND))\n"
                                + "GROUP BY ROLLUP(`name`), window_start, window_end")
                .await();
        return TestValuesTableFactory.getResultsAsStrings(sinkName);
    }

    /**
     * Chained in front of the sink, so it lives inside the final aggregation task: the open delay
     * keeps that task INITIALIZING while the source emits (checkpoint 1 then captures the records
     * as unaligned channel state), the per-row delay sustains backpressure while checkpoints race
     * the end-of-input watermark.
     */
    public static class ThrottlingFunction extends ScalarFunction {

        @Override
        public void open(FunctionContext context) throws Exception {
            Thread.sleep(SINK_OPEN_DELAY_MS);
        }

        public Long eval(Long value) {
            try {
                Thread.sleep(SINK_ROW_DELAY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return value;
        }
    }
}
