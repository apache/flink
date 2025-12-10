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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ParallelismOverrideUtil}. */
class ParallelismOverrideUtilTest {

    @Test
    void testApplyOverridesFromJobMasterConfiguration() {
        JobVertex vertex1 = new JobVertex("vertex1");
        vertex1.setParallelism(1);
        JobVertex vertex2 = new JobVertex("vertex2");
        vertex2.setParallelism(2);

        JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex1, vertex2);

        Configuration jobMasterConfig = new Configuration();
        Map<String, String> overrides = new HashMap<>();
        overrides.put(vertex1.getID().toHexString(), "10");
        overrides.put(vertex2.getID().toHexString(), "20");
        jobMasterConfig.set(PipelineOptions.PARALLELISM_OVERRIDES, overrides);

        ParallelismOverrideUtil.applyParallelismOverrides(jobGraph, jobMasterConfig);

        assertThat(vertex1.getParallelism()).isEqualTo(10);
        assertThat(vertex2.getParallelism()).isEqualTo(20);
    }

    @Test
    void testApplyOverridesFromJobGraphConfiguration() {
        JobVertex vertex1 = new JobVertex("vertex1");
        vertex1.setParallelism(1);
        JobVertex vertex2 = new JobVertex("vertex2");
        vertex2.setParallelism(2);

        JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex1, vertex2);

        Map<String, String> overrides = new HashMap<>();
        overrides.put(vertex1.getID().toHexString(), "10");
        overrides.put(vertex2.getID().toHexString(), "20");
        jobGraph.getJobConfiguration().set(PipelineOptions.PARALLELISM_OVERRIDES, overrides);

        ParallelismOverrideUtil.applyParallelismOverrides(jobGraph, new Configuration());

        assertThat(vertex1.getParallelism()).isEqualTo(10);
        assertThat(vertex2.getParallelism()).isEqualTo(20);
    }

    @Test
    void testJobGraphConfigurationTakesPrecedenceOverJobMasterConfiguration() {
        JobVertex vertex = new JobVertex("vertex");
        vertex.setParallelism(1);

        JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex);

        // Set override in job master configuration
        Configuration jobMasterConfig = new Configuration();
        jobMasterConfig.set(
                PipelineOptions.PARALLELISM_OVERRIDES, Map.of(vertex.getID().toHexString(), "10"));

        // Set different override in job graph configuration (should win)
        jobGraph.getJobConfiguration()
                .set(
                        PipelineOptions.PARALLELISM_OVERRIDES,
                        Map.of(vertex.getID().toHexString(), "20"));

        ParallelismOverrideUtil.applyParallelismOverrides(jobGraph, jobMasterConfig);

        assertThat(vertex.getParallelism()).isEqualTo(20);
    }

    @Test
    void testPartialOverrides() {
        JobVertex vertex1 = new JobVertex("vertex1");
        vertex1.setParallelism(1);
        JobVertex vertex2 = new JobVertex("vertex2");
        vertex2.setParallelism(2);

        JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex1, vertex2);

        // Only override vertex1, not vertex2
        Configuration jobMasterConfig = new Configuration();
        jobMasterConfig.set(
                PipelineOptions.PARALLELISM_OVERRIDES, Map.of(vertex1.getID().toHexString(), "10"));

        ParallelismOverrideUtil.applyParallelismOverrides(jobGraph, jobMasterConfig);

        assertThat(vertex1.getParallelism()).isEqualTo(10);
        assertThat(vertex2.getParallelism()).isEqualTo(2); // unchanged
    }

    @Test
    void testUnknownVertexIdIgnored() {
        JobVertex vertex = new JobVertex("vertex");
        vertex.setParallelism(1);

        JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex);

        Configuration jobMasterConfig = new Configuration();
        Map<String, String> overrides = new HashMap<>();
        overrides.put("non-existent-vertex-id", "10");
        overrides.put(vertex.getID().toHexString(), "20");
        jobMasterConfig.set(PipelineOptions.PARALLELISM_OVERRIDES, overrides);

        ParallelismOverrideUtil.applyParallelismOverrides(jobGraph, jobMasterConfig);

        // Should apply known vertex and ignore unknown one without error
        assertThat(vertex.getParallelism()).isEqualTo(20);
    }

    @Test
    void testNoOverridesWhenEmpty() {
        JobVertex vertex = new JobVertex("vertex");
        vertex.setParallelism(5);

        JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex);

        ParallelismOverrideUtil.applyParallelismOverrides(jobGraph, new Configuration());

        assertThat(vertex.getParallelism()).isEqualTo(5); // unchanged
    }

    @Test
    void testMultipleVerticesWithMixedOverrides() {
        JobVertex vertex1 = new JobVertex("vertex1");
        vertex1.setParallelism(1);
        JobVertex vertex2 = new JobVertex("vertex2");
        vertex2.setParallelism(2);
        JobVertex vertex3 = new JobVertex("vertex3");
        vertex3.setParallelism(3);

        JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex1, vertex2, vertex3);

        // Set some overrides in job master config
        Configuration jobMasterConfig = new Configuration();
        Map<String, String> masterOverrides = new HashMap<>();
        masterOverrides.put(vertex1.getID().toHexString(), "10");
        masterOverrides.put(vertex2.getID().toHexString(), "20");
        jobMasterConfig.set(PipelineOptions.PARALLELISM_OVERRIDES, masterOverrides);

        // Set some overrides in job config (vertex2 should be overridden by this)
        Map<String, String> jobOverrides = new HashMap<>();
        jobOverrides.put(vertex2.getID().toHexString(), "200");
        jobOverrides.put(vertex3.getID().toHexString(), "30");
        jobGraph.getJobConfiguration().set(PipelineOptions.PARALLELISM_OVERRIDES, jobOverrides);

        ParallelismOverrideUtil.applyParallelismOverrides(jobGraph, jobMasterConfig);

        assertThat(vertex1.getParallelism()).isEqualTo(10); // from job master config
        assertThat(vertex2.getParallelism()).isEqualTo(200); // job config wins
        assertThat(vertex3.getParallelism()).isEqualTo(30); // from job config
    }
}
