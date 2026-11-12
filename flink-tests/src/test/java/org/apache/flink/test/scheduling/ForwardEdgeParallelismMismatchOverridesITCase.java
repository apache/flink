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

package org.apache.flink.test.scheduling;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.JobManagerOptions.SchedulerType;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Changes the parallelism at submission time via {@link PipelineOptions#PARALLELISM_OVERRIDES},
 * which applies to a submitted JobGraph on all schedulers and to a submitted StreamGraph on the
 * streaming ones.
 */
class ForwardEdgeParallelismMismatchOverridesITCase
        extends ForwardEdgeParallelismMismatchITCaseBase {

    @Parameter(3)
    public boolean submitStreamGraph;

    @Parameters(name = "scheduler={0}, parallelism={1}, mode={2}, streamGraph={3}")
    private static List<Object[]> parameters() {
        final List<Object[]> parameters = new ArrayList<>();
        for (Object[] base :
                parameters(
                        Arrays.asList(
                                SchedulerType.Default,
                                SchedulerType.Adaptive,
                                SchedulerType.AdaptiveBatch),
                        false)) {
            parameters.add(new Object[] {base[0], base[1], base[2], false});
            // the adaptive batch scheduler does not apply the overrides to a StreamGraph
            if (base[0] != SchedulerType.AdaptiveBatch) {
                parameters.add(new Object[] {base[0], base[1], base[2], true});
            }
        }
        return parameters;
    }

    /**
     * Runs a bounded job with the initial parallelism to the end, then submits it again with the
     * new parallelism applied via the overrides.
     */
    @Override
    Optional<Throwable> runAndRescale(MiniCluster miniCluster) throws Exception {
        assertThat(runToEnd(miniCluster, createJobGraph(createSource())))
                .as("failure of the job with the initial parallelism")
                .isEmpty();

        final JobGraph jobGraph = createJobGraph(createSource());
        final Map<String, String> overrides = new HashMap<>();
        overrides.put(
                sourceId(jobGraph).toHexString(), String.valueOf(parallelismChange.newSource));
        overrides.put(sinkId(jobGraph).toHexString(), String.valueOf(parallelismChange.newSink));
        final ExecutionPlan plan = submitStreamGraph ? createStreamGraph(createSource()) : jobGraph;
        plan.getJobConfiguration().set(PipelineOptions.PARALLELISM_OVERRIDES, overrides);
        return runToEnd(miniCluster, plan);
    }

    private static DataGeneratorSource<Long> createSource() {
        return new DataGeneratorSource<>(index -> index, 100, Types.LONG);
    }

    private Optional<Throwable> runToEnd(MiniCluster miniCluster, ExecutionPlan plan)
            throws Exception {
        miniCluster.submitJob(plan).get();
        final JobResult result = miniCluster.requestJobResult(plan.getJobID()).get();
        return result.getSerializedThrowable()
                .map(t -> t.deserializeError(getClass().getClassLoader()));
    }
}
