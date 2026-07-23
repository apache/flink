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

package org.apache.flink.test.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.Serial;
import java.time.Duration;
import java.util.Map;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end verification of the per-downstream-target {@code numRecordsOut} metric exposed by
 * {@code TaskIOMetricGroup} and surfaced through {@link IOMetricsInfo#getRecordsWrittenPerTarget()}
 * in the REST {@code /jobs/:jobid} response.
 *
 * <p>Scenarios are organized into nested classes so each one can spin up a {@link
 * MiniClusterExtension} with its own scheduler configuration:
 *
 * <ul>
 *   <li>{@link UnderDefaultScheduler} covers the default scheduler for streaming jobs: side output
 *       and broadcast fan-out.
 *   <li>{@link UnderAdaptiveBatchScheduler} covers {@code AdaptiveBatch} (which drives {@code
 *       AdaptiveGraphManager}); the test documents a known per-target limitation under that mode.
 *   <li>{@link UnderAdaptiveStreamingScheduler} covers the streaming {@code Adaptive} scheduler ,
 *       which builds the full {@code JobGraph} upfront so per-target counters work end-to-end just
 *       like the default scheduler.
 * </ul>
 *
 * <p>Assertions go through the REST API to validate the complete pipeline: runtime metric
 * registration → {@code IOMetrics} snapshot → {@code MutableIOMetrics} per-subtask aggregation →
 * {@code IOMetricsInfo} JSON round-trip.
 *
 * <p>Each nested class uses {@link TestInstance.Lifecycle#PER_CLASS} so that an instance-level
 * {@code @RegisterExtension} field (required by {@link Nested} non-static inner classes under Java
 * 11 source level) still receives {@code beforeAll} callbacks and the underlying mini cluster is
 * started once per nested class.
 */
class PerTargetNumRecordsOutITCase {

    private static final int NUM_RECORDS = 20;
    private static final int SIDE_OUTPUT_PREDICATE = 3; // keep every (i % 3 == 0) for side output

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class UnderDefaultScheduler {

        @RegisterExtension
        private final MiniClusterExtension miniClusterExtension =
                new MiniClusterExtension(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(8)
                                .build());

        @Test
        void testPerTargetCountsWithSideOutput(
                @InjectClusterClient RestClusterClient<?> restClusterClient) throws Exception {
            final OutputTag<Long> sideTag = new OutputTag<Long>("side") {};

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.disableOperatorChaining();

            final DataStream<Long> source =
                    env.fromSource(
                                    new NumberSequenceSource(0, NUM_RECORDS - 1),
                                    WatermarkStrategy.noWatermarks(),
                                    "source-vertex")
                            .returns(BasicTypeInfo.LONG_TYPE_INFO);

            final SingleOutputStreamOperator<Long> splitter =
                    source.process(
                                    new ProcessFunction<Long, Long>() {
                                        @Serial private static final long serialVersionUID = 1L;

                                        @Override
                                        public void processElement(
                                                Long value, Context ctx, Collector<Long> out) {
                                            if (value % SIDE_OUTPUT_PREDICATE == 0) {
                                                ctx.output(sideTag, value);
                                            } else {
                                                out.collect(value);
                                            }
                                        }
                                    })
                            .name("splitter-vertex");

            splitter.sinkTo(new DiscardingSink<>()).name("main-sink-vertex");
            splitter.getSideOutput(sideTag).sinkTo(new DiscardingSink<>()).name("side-sink-vertex");

            final long expectedSide = countIf(value -> value % SIDE_OUTPUT_PREDICATE == 0);
            final long expectedMain = NUM_RECORDS - expectedSide;

            final JobClient jobClient = env.executeAsync("per-target-side-output");
            final JobID jobId = jobClient.getJobID();
            jobClient.getJobExecutionResult().get();

            final JobDetailsInfo jobDetails = fetchJobDetails(restClusterClient, jobId);

            final JobDetailsInfo.JobVertexDetailsInfo splitterVertex =
                    findVertex(jobDetails, "splitter-vertex");
            final JobVertexID mainSinkId =
                    findVertex(jobDetails, "main-sink-vertex").getJobVertexID();
            final JobVertexID sideSinkId =
                    findVertex(jobDetails, "side-sink-vertex").getJobVertexID();

            final IOMetricsInfo metrics = splitterVertex.getJobVertexMetrics();
            assertThat(metrics.getRecordsWritten())
                    .as("aggregate write-records equals total emits")
                    .isEqualTo(NUM_RECORDS);

            final Map<String, Long> perTarget = metrics.getRecordsWrittenPerTarget();
            assertThat(perTarget)
                    .as("per-target map surfaces both downstreams")
                    .containsEntry(mainSinkId.toHexString(), expectedMain)
                    .containsEntry(sideSinkId.toHexString(), expectedSide);
            assertThat(perTarget.values().stream().mapToLong(Long::longValue).sum())
                    .as("per-target sum equals aggregate when emits are routed, not broadcast")
                    .isEqualTo(NUM_RECORDS);
        }

        @Test
        void testPerTargetCountsWithBroadcast(
                @InjectClusterClient RestClusterClient<?> restClusterClient) throws Exception {
            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.disableOperatorChaining();

            final DataStream<Long> source =
                    env.fromSource(
                                    new NumberSequenceSource(0, NUM_RECORDS - 1),
                                    WatermarkStrategy.noWatermarks(),
                                    "broadcast-source-vertex")
                            .returns(BasicTypeInfo.LONG_TYPE_INFO);

            final DataStream<Long> broadcast = source.broadcast();
            broadcast.map(x -> x).name("fanout-a-vertex").sinkTo(new DiscardingSink<>());
            broadcast.map(x -> x).name("fanout-b-vertex").sinkTo(new DiscardingSink<>());
            broadcast.map(x -> x).name("fanout-c-vertex").sinkTo(new DiscardingSink<>());

            final JobClient jobClient = env.executeAsync("per-target-broadcast");
            final JobID jobId = jobClient.getJobID();
            jobClient.getJobExecutionResult().get();

            final JobDetailsInfo jobDetails = fetchJobDetails(restClusterClient, jobId);

            final JobDetailsInfo.JobVertexDetailsInfo sourceVertex =
                    findVertex(jobDetails, "broadcast-source-vertex");
            final JobVertexID fanoutA = findVertex(jobDetails, "fanout-a-vertex").getJobVertexID();
            final JobVertexID fanoutB = findVertex(jobDetails, "fanout-b-vertex").getJobVertexID();
            final JobVertexID fanoutC = findVertex(jobDetails, "fanout-c-vertex").getJobVertexID();

            final IOMetricsInfo metrics = sourceVertex.getJobVertexMetrics();
            assertThat(metrics.getRecordsWritten())
                    .as(
                            "aggregate write-records must count each logical emit once, "
                                    + "even when broadcast fans out to N targets")
                    .isEqualTo(NUM_RECORDS);

            final Map<String, Long> perTarget = metrics.getRecordsWrittenPerTarget();
            assertThat(perTarget)
                    .as("per-target map has one entry per broadcast fan-out target")
                    .containsEntry(fanoutA.toHexString(), (long) NUM_RECORDS)
                    .containsEntry(fanoutB.toHexString(), (long) NUM_RECORDS)
                    .containsEntry(fanoutC.toHexString(), (long) NUM_RECORDS)
                    .hasSize(3);
        }
    }

    /**
     * Same routed side-output topology as {@link UnderDefaultScheduler} but explicitly running
     * under the {@code AdaptiveBatch} scheduler (FLIP-187), which drives job-graph construction
     * through {@code AdaptiveGraphManager} in an incremental, per-stage fashion.
     *
     * <p>The expectation documents a <b>known limitation</b>: the upstream operator's {@code
     * OperatorChain} binds its per-target counters <em>once</em>, at task startup, by reading
     * {@code OP_NONCHAINED_OUTPUTS} from its {@code StreamConfig}. In adaptive batch the downstream
     * {@link JobVertexID} is not known until a <em>later</em> iteration of {@code
     * AdaptiveGraphManager} (triggered by the upstream finishing). By the time the planner-side
     * patching (see {@code AdaptiveGraphManager#connectToFinishedUpStreamVertex}) stamps the
     * downstream id into the cached {@link org.apache.flink.streaming.api.graph.NonChainedOutput},
     * the upstream task has already finished and its metrics are archived. Consequently the
     * per-target map is empty for upstream vertices whose downstream is not yet known at task
     * start; only the aggregate {@code numRecordsOut} is populated (and stays fully correct).
     *
     * <p>Consumers that need the per-target breakdown must treat an empty map as "breakdown
     * unavailable for this vertex in adaptive batch" and fall back to the aggregate.
     */
    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class UnderAdaptiveBatchScheduler {

        @RegisterExtension
        private final MiniClusterExtension miniClusterExtension =
                new MiniClusterExtension(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(adaptiveBatchConfiguration())
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(8)
                                .build());

        private Configuration adaptiveBatchConfiguration() {
            final Configuration configuration = new Configuration();
            configuration.set(
                    JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.AdaptiveBatch);
            return configuration;
        }

        @Test
        void testPerTargetCountsWithSideOutput(
                @InjectClusterClient RestClusterClient<?> restClusterClient) throws Exception {
            final OutputTag<Long> sideTag = new OutputTag<Long>("side") {};

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            env.setParallelism(1);
            env.disableOperatorChaining();

            final DataStream<Long> source =
                    env.fromSource(
                                    new NumberSequenceSource(0, NUM_RECORDS - 1),
                                    WatermarkStrategy.noWatermarks(),
                                    "adaptive-batch-source-vertex")
                            .returns(BasicTypeInfo.LONG_TYPE_INFO);

            final SingleOutputStreamOperator<Long> splitter =
                    source.process(
                                    new ProcessFunction<Long, Long>() {
                                        @Serial private static final long serialVersionUID = 1L;

                                        @Override
                                        public void processElement(
                                                Long value, Context ctx, Collector<Long> out) {
                                            if (value % SIDE_OUTPUT_PREDICATE == 0) {
                                                ctx.output(sideTag, value);
                                            } else {
                                                out.collect(value);
                                            }
                                        }
                                    })
                            .name("adaptive-batch-splitter-vertex");

            splitter.sinkTo(new DiscardingSink<>()).name("adaptive-batch-main-sink-vertex");
            splitter.getSideOutput(sideTag)
                    .sinkTo(new DiscardingSink<>())
                    .name("adaptive-batch-side-sink-vertex");

            final JobClient jobClient = env.executeAsync("per-target-adaptive-batch");
            final JobID jobId = jobClient.getJobID();
            jobClient.getJobExecutionResult().get();

            final JobDetailsInfo jobDetails = fetchJobDetails(restClusterClient, jobId);
            final JobDetailsInfo.JobVertexDetailsInfo splitterVertex =
                    findVertex(jobDetails, "adaptive-batch-splitter-vertex");
            final IOMetricsInfo metrics = splitterVertex.getJobVertexMetrics();

            assertThat(metrics.getRecordsWritten())
                    .as(
                            "aggregate write-records is fully populated even under adaptive"
                                    + " batch: the aggregate counter is independent of any"
                                    + " per-target key")
                    .isEqualTo(NUM_RECORDS);

            assertThat(metrics.getRecordsWrittenPerTarget())
                    .as(
                            "per-target map is empty under adaptive batch for this edge: the"
                                    + " downstream JobVertexID is not yet known at upstream-task"
                                    + " start, so the runtime OperatorChain cannot register a"
                                    + " per-target counter. This is a documented limitation of"
                                    + " the feature; planner-side patching still happens for any"
                                    + " external consumer that reads the serialized StreamConfig"
                                    + " after job completion.")
                    .isNotNull()
                    .isEmpty();
        }
    }

    /**
     * Same assertions as {@link UnderDefaultScheduler} (streaming cases), but under the streaming
     * {@code Adaptive} scheduler. The adaptive streaming scheduler builds the full {@code JobGraph}
     * upfront (unlike {@code AdaptiveGraphManager} for batch), so all downstream {@link
     * JobVertexID}s are known at upstream-task start time and per-target counters bind correctly
     * end-to-end; consequently the expectations match the default-scheduler tests.
     */
    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class UnderAdaptiveStreamingScheduler {

        @RegisterExtension
        private final MiniClusterExtension miniClusterExtension =
                new MiniClusterExtension(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(adaptiveStreamingConfiguration())
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(8)
                                .build());

        private Configuration adaptiveStreamingConfiguration() {
            final Configuration configuration = new Configuration();
            configuration.set(
                    JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
            // Keep the submission stabilization short so the test does not spend time waiting for
            // additional slots beyond what the mini-cluster already advertises.
            configuration.set(
                    JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT,
                    Duration.ofMillis(100L));
            return configuration;
        }

        @Test
        void testPerTargetCountsWithSideOutput(
                @InjectClusterClient RestClusterClient<?> restClusterClient) throws Exception {
            final OutputTag<Long> sideTag = new OutputTag<Long>("side") {};

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.disableOperatorChaining();

            final DataStream<Long> source =
                    env.fromSource(
                                    new NumberSequenceSource(0, NUM_RECORDS - 1),
                                    WatermarkStrategy.noWatermarks(),
                                    "adaptive-stream-source")
                            .returns(BasicTypeInfo.LONG_TYPE_INFO);

            final SingleOutputStreamOperator<Long> splitter =
                    source.process(
                                    new ProcessFunction<Long, Long>() {
                                        @Serial private static final long serialVersionUID = 1L;

                                        @Override
                                        public void processElement(
                                                Long value, Context ctx, Collector<Long> out) {
                                            if (value % SIDE_OUTPUT_PREDICATE == 0) {
                                                ctx.output(sideTag, value);
                                            } else {
                                                out.collect(value);
                                            }
                                        }
                                    })
                            .name("adaptive-stream-splitter");

            splitter.sinkTo(new DiscardingSink<>()).name("adaptive-stream-main-sink");
            splitter.getSideOutput(sideTag)
                    .sinkTo(new DiscardingSink<>())
                    .name("adaptive-stream-side-sink");

            final long expectedSide = countIf(value -> value % SIDE_OUTPUT_PREDICATE == 0);
            final long expectedMain = NUM_RECORDS - expectedSide;

            final JobClient jobClient = env.executeAsync("per-target-adaptive-streaming");
            final JobID jobId = jobClient.getJobID();
            jobClient.getJobExecutionResult().get();

            final JobDetailsInfo jobDetails = fetchJobDetails(restClusterClient, jobId);

            final JobDetailsInfo.JobVertexDetailsInfo splitterVertex =
                    findVertex(jobDetails, "adaptive-stream-splitter");
            final JobVertexID mainSinkId =
                    findVertex(jobDetails, "adaptive-stream-main-sink").getJobVertexID();
            final JobVertexID sideSinkId =
                    findVertex(jobDetails, "adaptive-stream-side-sink").getJobVertexID();

            final IOMetricsInfo metrics = splitterVertex.getJobVertexMetrics();
            assertThat(metrics.getRecordsWritten())
                    .as("aggregate write-records equals total emits under adaptive streaming")
                    .isEqualTo(NUM_RECORDS);

            final Map<String, Long> perTarget = metrics.getRecordsWrittenPerTarget();
            assertThat(perTarget)
                    .as(
                            "per-target map is fully populated under the streaming Adaptive"
                                    + " scheduler: the full JobGraph is built upfront, so all"
                                    + " downstream JobVertexIDs are known when the upstream task"
                                    + " binds its per-target counters")
                    .containsEntry(mainSinkId.toHexString(), expectedMain)
                    .containsEntry(sideSinkId.toHexString(), expectedSide);
            assertThat(perTarget.values().stream().mapToLong(Long::longValue).sum())
                    .as("per-target sum equals aggregate when emits are routed, not broadcast")
                    .isEqualTo(NUM_RECORDS);
        }

        @Test
        void testPerTargetCountsWithBroadcast(
                @InjectClusterClient RestClusterClient<?> restClusterClient) throws Exception {
            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.disableOperatorChaining();

            final DataStream<Long> source =
                    env.fromSource(
                                    new NumberSequenceSource(0, NUM_RECORDS - 1),
                                    WatermarkStrategy.noWatermarks(),
                                    "adaptive-stream-broadcast-source")
                            .returns(BasicTypeInfo.LONG_TYPE_INFO);

            final DataStream<Long> broadcast = source.broadcast();
            broadcast.map(x -> x).name("adaptive-stream-fanout-a").sinkTo(new DiscardingSink<>());
            broadcast.map(x -> x).name("adaptive-stream-fanout-b").sinkTo(new DiscardingSink<>());
            broadcast.map(x -> x).name("adaptive-stream-fanout-c").sinkTo(new DiscardingSink<>());

            final JobClient jobClient = env.executeAsync("per-target-adaptive-streaming-broadcast");
            final JobID jobId = jobClient.getJobID();
            jobClient.getJobExecutionResult().get();

            final JobDetailsInfo jobDetails = fetchJobDetails(restClusterClient, jobId);

            final JobDetailsInfo.JobVertexDetailsInfo sourceVertex =
                    findVertex(jobDetails, "adaptive-stream-broadcast-source");
            final JobVertexID fanoutA =
                    findVertex(jobDetails, "adaptive-stream-fanout-a").getJobVertexID();
            final JobVertexID fanoutB =
                    findVertex(jobDetails, "adaptive-stream-fanout-b").getJobVertexID();
            final JobVertexID fanoutC =
                    findVertex(jobDetails, "adaptive-stream-fanout-c").getJobVertexID();

            final IOMetricsInfo metrics = sourceVertex.getJobVertexMetrics();
            assertThat(metrics.getRecordsWritten())
                    .as(
                            "aggregate write-records counts each logical emit once, even when"
                                    + " broadcast fans out to N targets under adaptive streaming")
                    .isEqualTo(NUM_RECORDS);

            assertThat(metrics.getRecordsWrittenPerTarget())
                    .as("per-target map has one entry per broadcast fan-out target")
                    .containsEntry(fanoutA.toHexString(), (long) NUM_RECORDS)
                    .containsEntry(fanoutB.toHexString(), (long) NUM_RECORDS)
                    .containsEntry(fanoutC.toHexString(), (long) NUM_RECORDS)
                    .hasSize(3);
        }
    }

    private static JobDetailsInfo fetchJobDetails(
            RestClusterClient<?> restClusterClient, JobID jobId) throws Exception {
        // The aggregated archived IOMetrics are populated when the ExecutionGraph transitions
        // to a terminal state. getJobExecutionResult() already awaits that, but the REST handler
        // reads from the ExecutionGraphCache which may briefly be empty for the just-finished
        // job; poll until the response exposes the finished job's vertices.
        final JobDetailsInfo[] result = new JobDetailsInfo[1];
        waitUntilCondition(
                () -> {
                    result[0] = restClusterClient.getJobDetails(jobId).get();
                    return result[0] != null && !result[0].getJobVertexInfos().isEmpty();
                },
                10_000);
        return result[0];
    }

    private static JobDetailsInfo.JobVertexDetailsInfo findVertex(
            JobDetailsInfo details, String nameContains) {
        return details.getJobVertexInfos().stream()
                .filter(v -> v.getName().contains(nameContains))
                .findFirst()
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        "No job vertex with name containing " + nameContains));
    }

    private static long countIf(java.util.function.LongPredicate pred) {
        long count = 0;
        for (int i = 0; i < NUM_RECORDS; i++) {
            if (pred.test(i)) {
                count++;
            }
        }
        return count;
    }
}
