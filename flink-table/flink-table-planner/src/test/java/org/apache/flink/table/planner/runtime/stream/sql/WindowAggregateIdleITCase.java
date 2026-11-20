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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.CheckpointStatsCounts;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.util.source.AbstractTestSource;
import org.apache.flink.test.util.source.TestSourceReader;
import org.apache.flink.test.util.source.TestSplit;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.assertj.core.api.Assertions.assertThat;

/** SQL job coverage for flushing local window aggregates before an input becomes idle. */
@Execution(ExecutionMode.SAME_THREAD)
class WindowAggregateIdleITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @RegisterExtension
    private final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    private static final LocalDateTime EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0);

    @AfterEach
    void clearResults() {
        TestValuesTableFactory.clearAllData();
    }

    @ParameterizedTest(name = "{0}, {1}, checkpointBeforeIdle={2}")
    @CsvSource({
        "ONE_PHASE, TUMBLE, false",
        "TWO_PHASE, TUMBLE, false",
        "ONE_PHASE, HOP, false",
        "TWO_PHASE, HOP, false",
        "ONE_PHASE, CUMULATE, false",
        "TWO_PHASE, CUMULATE, false",
        "TWO_PHASE, TUMBLE, true"
    })
    void testIdleWindows(
            AggregatePhaseStrategy phase,
            String window,
            boolean checkpointBeforeIdle,
            @InjectMiniCluster MiniCluster cluster)
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // Prevent periodic checkpoints from flushing buffers before the idle transition.
        env.enableCheckpointing(Long.MAX_VALUE);
        final StreamTableEnvironment tables = StreamTableEnvironment.create(env);
        tables.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, phase);
        final List<FutureCompletingBlockingQueue<Action>> queues =
                Arrays.asList(
                        new FutureCompletingBlockingQueue<>(),
                        new FutureCompletingBlockingQueue<>());
        final DataStream<Row> input =
                env.fromSource(
                        new ControlledSource(sharedObjects.add(queues)),
                        WatermarkStrategy.noWatermarks(),
                        "Controlled input",
                        Types.ROW_NAMED(
                                new String[] {"k", "amount", "ts"},
                                Types.STRING,
                                Types.INT,
                                Types.LOCAL_DATE_TIME));
        tables.createTemporaryView(
                "input_table",
                tables.fromDataStream(
                        input,
                        Schema.newBuilder()
                                .column("k", STRING())
                                .column("amount", INT())
                                .column("ts", TIMESTAMP(3))
                                .watermark("ts", "SOURCE_WATERMARK()")
                                .build()));
        tables.executeSql(
                "CREATE TABLE results (k STRING, window_start TIMESTAMP(3), "
                        + "window_end TIMESTAMP(3), cnt BIGINT, total BIGINT) "
                        + "WITH ('connector' = 'values', 'sink-insert-only' = 'true')");
        final String intervals =
                window.equals("TUMBLE")
                        ? "INTERVAL '10' SECOND"
                        : "INTERVAL '5' SECOND, INTERVAL '10' SECOND";
        final String query =
                "INSERT INTO results SELECT k, window_start, window_end, COUNT(*), SUM(amount) "
                        + "FROM TABLE("
                        + window
                        + "(TABLE input_table, DESCRIPTOR(ts), "
                        + intervals
                        + ")) "
                        + "GROUP BY k, window_start, window_end";
        final String plan = tables.explainSql(query);
        if (phase == AggregatePhaseStrategy.TWO_PHASE) {
            assertThat(plan).contains("LocalWindowAggregate", "GlobalWindowAggregate");
        } else {
            assertThat(plan).doesNotContain("LocalWindowAggregate", "GlobalWindowAggregate");
        }
        final JobClient job =
                tables.executeSql(query).getJobClient().orElseThrow(IllegalStateException::new);
        try {
            waitForAllTaskRunning(cluster, job.getJobID(), false);
            if (phase == AggregatePhaseStrategy.TWO_PHASE) {
                assertThat(
                                cluster.getExecutionGraph(job.getJobID())
                                        .get(30, SECONDS)
                                        .getAllVertices()
                                        .values())
                        .anySatisfy(
                                vertex ->
                                        assertThat(vertex.getName())
                                                .contains(
                                                        "Source: Controlled input",
                                                        "LocalWindowAggregate"));
            }
            send(
                    queues.get(1),
                    output -> {
                        output.collect(record("first", 101, 1));
                        output.emitWatermark(new Watermark(0));
                    });
            send(
                    queues.get(0),
                    output -> {
                        output.collect(record("first", 10, 1));
                        output.collect(record("first", 10, 4));
                        output.collect(record("first", 10, 8));
                        output.emitWatermark(new Watermark(13000));
                    });
            if (checkpointBeforeIdle) {
                cluster.triggerCheckpoint(job.getJobID()).get(30, SECONDS);
            }
            send(queues.get(1), ReaderOutput::markIdle);
            final List<Row> expected = new ArrayList<>();
            if (window.equals("HOP")) {
                expected.add(result("first", -5, 5, 3, 121));
            } else if (window.equals("CUMULATE")) {
                expected.add(result("first", 0, 5, 3, 121));
            }
            expected.add(result("first", 0, 10, 4, 131));
            assertResults(expected);
            assertCheckpointCount(cluster, job, checkpointBeforeIdle ? 1 : 0);
            cluster.triggerCheckpoint(job.getJobID()).get(30, SECONDS);

            // Re-align the resumed input before testing a second idle transition.
            send(
                    queues.get(1),
                    output -> {
                        output.markActive();
                        output.emitWatermark(new Watermark(20000));
                    });
            send(queues.get(0), output -> output.emitWatermark(new Watermark(20000)));
            if (window.equals("HOP")) {
                expected.add(result("first", 5, 15, 1, 10));
            }
            assertResults(expected);
            cluster.triggerCheckpoint(job.getJobID()).get(30, SECONDS);
            send(
                    queues.get(1),
                    output -> {
                        output.collect(record("resumed", 7, 21));
                        output.collect(record("resumed", 13, 24));
                    });
            send(
                    queues.get(0),
                    output -> {
                        output.collect(record("resumed", 11, 21));
                        output.emitWatermark(new Watermark(33000));
                    });
            send(queues.get(1), ReaderOutput::markIdle);
            if (window.equals("HOP")) {
                expected.add(result("resumed", 15, 25, 3, 31));
            } else if (window.equals("CUMULATE")) {
                expected.add(result("resumed", 20, 25, 3, 31));
            }
            expected.add(result("resumed", 20, 30, 3, 31));
            assertResults(expected);

            // Truly late input must not reopen the already completed windows.
            send(queues.get(0), output -> output.collect(record("first", 999, 1)));
            send(queues.get(0), ReaderOutput::markIdle);
            cluster.triggerCheckpoint(job.getJobID()).get(30, SECONDS);
            assertCheckpointCount(cluster, job, checkpointBeforeIdle ? 4 : 3);
            assertThat(TestValuesTableFactory.getRawResults("results"))
                    .containsExactlyInAnyOrderElementsOf(expected);
        } finally {
            job.cancel().get(30, SECONDS);
        }
    }

    private static void assertCheckpointCount(
            MiniCluster cluster, JobClient job, long expectedCount) throws Exception {
        final CheckpointStatsCounts counts =
                cluster.getExecutionGraph(job.getJobID())
                        .get(30, SECONDS)
                        .getCheckpointStatsSnapshot()
                        .getCounts();
        assertThat(counts.getTotalNumberOfCheckpoints()).isEqualTo(expectedCount);
        assertThat(counts.getNumberOfCompletedCheckpoints()).isEqualTo(expectedCount);
    }

    private static Row record(String key, int amount, int second) {
        return Row.of(key, amount, EPOCH.plusSeconds(second));
    }

    private static Row result(String key, int start, int end, long count, long sum) {
        return Row.of(key, EPOCH.plusSeconds(start), EPOCH.plusSeconds(end), count, sum);
    }

    private static void assertResults(List<Row> expected) throws Exception {
        waitUntilCondition(
                () -> TestValuesTableFactory.getRawResults("results").size() >= expected.size(),
                50,
                600);
        assertThat(TestValuesTableFactory.getRawResults("results"))
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    private static void send(
            FutureCompletingBlockingQueue<Action> queue, Consumer<ReaderOutput<Row>> emit)
            throws Exception {
        final Action action = new Action(emit);
        queue.put(0, action);
        action.processed.get(30, SECONDS);
    }

    private static class Action {
        private final Consumer<ReaderOutput<Row>> emit;
        private final CompletableFuture<Void> processed = new CompletableFuture<>();

        private Action(Consumer<ReaderOutput<Row>> emit) {
            this.emit = emit;
        }
    }

    private static class ControlledSource extends AbstractTestSource<Row> {
        private static final long serialVersionUID = 1L;
        private final SharedReference<List<FutureCompletingBlockingQueue<Action>>> queues;

        private ControlledSource(
                SharedReference<List<FutureCompletingBlockingQueue<Action>>> queues) {
            this.queues = queues;
        }

        @Override
        public SourceReader<Row, TestSplit> createReader(SourceReaderContext context) {
            final FutureCompletingBlockingQueue<Action> queue =
                    queues.get().get(context.getIndexOfSubtask());
            return new TestSourceReader<Row>(context) {
                @Override
                public InputStatus pollNext(ReaderOutput<Row> output) {
                    final Action action = queue.poll();
                    if (action == null) {
                        return InputStatus.NOTHING_AVAILABLE;
                    }
                    try {
                        action.emit.accept(output);
                        action.processed.complete(null);
                    } catch (RuntimeException failure) {
                        action.processed.completeExceptionally(failure);
                        throw failure;
                    }
                    return InputStatus.MORE_AVAILABLE;
                }

                @Override
                public CompletableFuture<Void> isAvailable() {
                    return queue.getAvailabilityFuture();
                }
            };
        }
    }
}
