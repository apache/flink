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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.client.program.rest.RestClusterClient;
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.Serial;
import java.util.Map;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end verification of the per-downstream-target {@code numRecordsOut} metric exposed by
 * {@code TaskIOMetricGroup} and surfaced through {@link IOMetricsInfo#getRecordsWrittenPerTarget()}
 * in the REST {@code /jobs/:jid} response.
 *
 * <p>Two topologies are exercised:
 *
 * <ul>
 *   <li>Side output: one upstream operator routes distinct record subsets to two downstream
 *       vertices. Per-target counts must reflect the per-route counts and their sum must equal the
 *       aggregate {@code write-records}.
 *   <li>Broadcast: one upstream fans the same records out to three downstream vertices. Per-target
 *       counts must each equal the aggregate, and the aggregate must <em>not</em> double-count
 *       emits.
 * </ul>
 *
 * <p>Assertions go through the REST API to validate the complete pipeline: runtime metric
 * registration → {@code IOMetrics} snapshot → {@code MutableIOMetrics} per-subtask aggregation →
 * {@code IOMetricsInfo} JSON round-trip.
 */
class PerTargetNumRecordsOutITCase {

    private static final int NUM_RECORDS = 20;
    private static final int SIDE_OUTPUT_PREDICATE = 3; // keep every (i % 3 == 0) for side output

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(8)
                            .build());

    @Test
    void testPerTargetCountsWithSideOutput(
            @InjectClusterClient RestClusterClient<?> restClusterClient) throws Exception {
        final OutputTag<Long> sideTag = new OutputTag<Long>("side") {};

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        final long expectedSide = countIf(NUM_RECORDS, value -> value % SIDE_OUTPUT_PREDICATE == 0);
        final long expectedMain = NUM_RECORDS - expectedSide;

        final JobClient jobClient = env.executeAsync("per-target-side-output");
        final JobID jobId = jobClient.getJobID();
        jobClient.getJobExecutionResult().get();

        final JobDetailsInfo jobDetails = fetchJobDetails(restClusterClient, jobId);

        final JobDetailsInfo.JobVertexDetailsInfo splitterVertex =
                findVertex(jobDetails, "splitter-vertex");
        final JobVertexID mainSinkId = findVertex(jobDetails, "main-sink-vertex").getJobVertexID();
        final JobVertexID sideSinkId = findVertex(jobDetails, "side-sink-vertex").getJobVertexID();

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
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

    private static long countIf(int n, java.util.function.LongPredicate pred) {
        long count = 0;
        for (int i = 0; i < n; i++) {
            if (pred.test(i)) {
                count++;
            }
        }
        return count;
    }
}
