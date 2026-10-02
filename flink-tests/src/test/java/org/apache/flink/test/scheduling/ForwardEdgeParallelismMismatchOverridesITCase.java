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
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Changes the parallelism at submission time via {@link PipelineOptions#PARALLELISM_OVERRIDES},
 * which applies to all schedulers.
 */
class ForwardEdgeParallelismMismatchOverridesITCase
        extends ForwardEdgeParallelismMismatchITCaseBase {

    @Parameters(name = "scheduler={0}, parallelism={1}, mode={2}")
    private static List<Object[]> parameters() {
        return parameters(
                Arrays.asList(
                        SchedulerType.Default, SchedulerType.Adaptive, SchedulerType.AdaptiveBatch),
                false);
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
        jobGraph.getJobConfiguration().set(PipelineOptions.PARALLELISM_OVERRIDES, overrides);
        return runToEnd(miniCluster, jobGraph);
    }

    private static DataGeneratorSource<Long> createSource() {
        return new DataGeneratorSource<>(index -> index, 100, Types.LONG);
    }

    private Optional<Throwable> runToEnd(MiniCluster miniCluster, JobGraph jobGraph)
            throws Exception {
        miniCluster.submitJob(jobGraph).get();
        final JobResult result = miniCluster.requestJobResult(jobGraph.getJobID()).get();
        return result.getSerializedThrowable()
                .map(t -> t.deserializeError(getClass().getClassLoader()));
    }
}
