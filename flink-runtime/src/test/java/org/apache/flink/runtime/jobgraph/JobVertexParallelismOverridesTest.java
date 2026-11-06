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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JobVertexParallelismOverrides}. */
class JobVertexParallelismOverridesTest {

    @Test
    void testMergePrefersTheJobConfiguration() {
        final String onlyInCluster = new JobVertexID().toHexString();
        final String inBoth = new JobVertexID().toHexString();
        final String onlyInJob = new JobVertexID().toHexString();

        final Configuration clusterConfiguration = new Configuration();
        final Map<String, String> clusterOverrides = new HashMap<>();
        clusterOverrides.put(onlyInCluster, "1");
        clusterOverrides.put(inBoth, "2");
        clusterConfiguration.set(PipelineOptions.PARALLELISM_OVERRIDES, clusterOverrides);

        final Configuration jobConfiguration = new Configuration();
        final Map<String, String> jobOverrides = new HashMap<>();
        jobOverrides.put(inBoth, "3");
        jobOverrides.put(onlyInJob, "4");
        jobConfiguration.set(PipelineOptions.PARALLELISM_OVERRIDES, jobOverrides);

        final Map<String, String> expected = new HashMap<>();
        expected.put(onlyInCluster, "1");
        expected.put(inBoth, "3");
        expected.put(onlyInJob, "4");
        assertThat(JobVertexParallelismOverrides.merge(clusterConfiguration, jobConfiguration))
                .isEqualTo(expected);
    }

    @Test
    void testMergeOfEmptyConfigurationsIsEmpty() {
        assertThat(JobVertexParallelismOverrides.merge(new Configuration(), new Configuration()))
                .isEmpty();
    }

    @Test
    void testByJobVertexIdKeepsWellFormedKeysAndDropsTheOthers() {
        final JobVertexID id = new JobVertexID();
        final Map<String, String> overrides = new HashMap<>();
        overrides.put(id.toHexString(), "7");
        overrides.put("v1", "3");
        overrides.put(id.toHexString() + "0", "3");
        overrides.put(id.toHexString().toUpperCase(), "3");

        assertThat(JobVertexParallelismOverrides.byJobVertexId(overrides))
                .isEqualTo(Collections.singletonMap(id, "7"));
    }

    @Test
    void testToConfigurationValueInvertsByJobVertexId() {
        final JobVertexID id = new JobVertexID();
        final Map<String, String> overrides = Collections.singletonMap(id.toHexString(), "7");

        assertThat(
                        JobVertexParallelismOverrides.toConfigurationValue(
                                JobVertexParallelismOverrides.byJobVertexId(overrides)))
                .isEqualTo(overrides);
    }

    @ParameterizedTest(name = "value={0}")
    @CsvSource({"abc", "0", "-2", "''", "1.5"})
    void testApplyRejectsAValueThatIsNoParallelism(String value) {
        final JobVertex vertex = new JobVertex("vertex");
        vertex.setParallelism(1);

        assertThatThrownBy(
                        () ->
                                JobVertexParallelismOverrides.apply(
                                        vertex, Collections.singletonMap(vertex.getID(), value)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(vertex.getID().toString())
                .hasMessageContaining(PipelineOptions.PARALLELISM_OVERRIDES.key());
        assertThat(vertex.getParallelism()).isEqualTo(1);
    }

    @Test
    void testApplyAcceptsMinusOneAsTheDefaultParallelism() {
        final JobVertex vertex = new JobVertex("vertex");
        vertex.setParallelism(3);

        assertThat(
                        JobVertexParallelismOverrides.apply(
                                vertex, Collections.singletonMap(vertex.getID(), "-1")))
                .isTrue();
        assertThat(vertex.getParallelism()).isEqualTo(-1);
    }

    @Test
    void testApplyIgnoresABadValueForAnotherVertex() {
        final JobVertex vertex = new JobVertex("vertex");
        vertex.setParallelism(1);

        assertThat(
                        JobVertexParallelismOverrides.apply(
                                vertex, Collections.singletonMap(new JobVertexID(), "abc")))
                .isFalse();
        assertThat(vertex.getParallelism()).isEqualTo(1);
    }

    @Test
    void testApplyChangesOnlyTheOverriddenVertex() {
        final JobVertex overridden = new JobVertex("overridden");
        overridden.setParallelism(1);
        final JobVertex untouched = new JobVertex("untouched");
        untouched.setParallelism(2);
        final Map<JobVertexID, String> overrides =
                Collections.singletonMap(overridden.getID(), "5");

        assertThat(JobVertexParallelismOverrides.apply(overridden, overrides)).isTrue();
        assertThat(JobVertexParallelismOverrides.apply(untouched, overrides)).isFalse();
        assertThat(overridden.getParallelism()).isEqualTo(5);
        assertThat(untouched.getParallelism()).isEqualTo(2);
    }
}
