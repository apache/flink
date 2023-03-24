/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.flink.runtime.jobgraph.JobResourceRequirements}. */
@ExtendWith(TestLoggerExtension.class)
class JobResourceRequirementsTest {

    private final JobVertexID firstVertexId = new JobVertexID();
    private final JobVertexID secondVertexId = new JobVertexID();

    @Test
    void testSuccessfulValidation() {
        final JobResourceRequirements jobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(firstVertexId, 1, 4)
                        .setParallelismForJobVertex(secondVertexId, 1, 4)
                        .build();
        final Map<JobVertexID, Integer> maxParallelismPerVertex = new HashMap<>();
        maxParallelismPerVertex.put(firstVertexId, 10);
        maxParallelismPerVertex.put(secondVertexId, 10);
        final List<String> validationErrors =
                JobResourceRequirements.validate(jobResourceRequirements, maxParallelismPerVertex);
        assertThat(validationErrors).isEmpty();
    }

    @Test
    void testValidateVertexIdsNotFoundInJobGraph() {
        final JobResourceRequirements jobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(firstVertexId, 1, 4)
                        .setParallelismForJobVertex(secondVertexId, 1, 4)
                        .build();
        final List<String> validationErrors =
                JobResourceRequirements.validate(jobResourceRequirements, Collections.emptyMap());
        assertThat(validationErrors).hasSize(2);
        for (String validationError : validationErrors) {
            assertThat(validationError).contains("was not found in the JobGraph");
        }
    }

    @Test
    void testValidateUpperBoundHigherThanMaxParallelism() {
        final JobResourceRequirements jobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(firstVertexId, 1, 10)
                        .setParallelismForJobVertex(secondVertexId, 1, 5)
                        .build();
        final Map<JobVertexID, Integer> maxParallelismPerVertex = new HashMap<>();
        maxParallelismPerVertex.put(firstVertexId, 5);
        maxParallelismPerVertex.put(secondVertexId, 5);
        final List<String> validationErrors =
                JobResourceRequirements.validate(jobResourceRequirements, maxParallelismPerVertex);
        assertThat(validationErrors).hasSize(1);
        for (String validationError : validationErrors) {
            assertThat(validationError).contains("exceeds its maximum parallelism");
        }
    }

    @Test
    void testValidateIncompleteRequirements() {
        final JobResourceRequirements jobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(firstVertexId, 1, 10)
                        .build();
        final Map<JobVertexID, Integer> maxParallelismPerVertex = new HashMap<>();
        maxParallelismPerVertex.put(firstVertexId, 10);
        maxParallelismPerVertex.put(secondVertexId, 10);
        final List<String> validationErrors =
                JobResourceRequirements.validate(jobResourceRequirements, maxParallelismPerVertex);
        assertThat(validationErrors).hasSize(1);
        for (String validationError : validationErrors) {
            assertThat(validationError).contains("request is incomplete");
        }
    }

    @Test
    void testValidateLowerBoundDoesNotExceedUpperBound() {
        final JobResourceRequirements jobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(firstVertexId, 10, 9)
                        .setParallelismForJobVertex(secondVertexId, 10, 10)
                        .build();
        final Map<JobVertexID, Integer> maxParallelismPerVertex = new HashMap<>();
        maxParallelismPerVertex.put(firstVertexId, 10);
        maxParallelismPerVertex.put(secondVertexId, 10);
        final List<String> validationErrors =
                JobResourceRequirements.validate(jobResourceRequirements, maxParallelismPerVertex);
        assertThat(validationErrors).hasSize(1);
        for (String validationError : validationErrors) {
            assertThat(validationError).contains("is higher than the upper bound");
        }
    }

    @Test
    void testValidateLowerOrUpperBoundIsLowerThanOne() {
        final JobResourceRequirements jobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(firstVertexId, 0, 10)
                        .setParallelismForJobVertex(secondVertexId, 1, 0)
                        .build();
        final Map<JobVertexID, Integer> maxParallelismPerVertex = new HashMap<>();
        maxParallelismPerVertex.put(firstVertexId, 10);
        maxParallelismPerVertex.put(secondVertexId, 10);
        final List<String> validationErrors =
                JobResourceRequirements.validate(jobResourceRequirements, maxParallelismPerVertex);
        assertThat(validationErrors).hasSize(2);
        for (String validationError : validationErrors) {
            assertThat(validationError).contains("must be greater than zero");
        }
    }

    @Test
    void testValidateDefaults() {
        final JobResourceRequirements jobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(firstVertexId, -1, -1)
                        .build();
        final Map<JobVertexID, Integer> maxParallelismPerVertex = new HashMap<>();
        maxParallelismPerVertex.put(firstVertexId, 10);
        final List<String> validationErrors =
                JobResourceRequirements.validate(jobResourceRequirements, maxParallelismPerVertex);
        assertThat(validationErrors).isEmpty();
    }

    @Test
    void testWriteToJobGraphAndReadFromJobGraph() throws IOException {
        final JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();
        final JobResourceRequirements jobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(firstVertexId, 10, 9)
                        .setParallelismForJobVertex(secondVertexId, 10, 10)
                        .build();

        JobResourceRequirements.writeToJobGraph(jobGraph, jobResourceRequirements);
        assertThat(JobResourceRequirements.readFromJobGraph(jobGraph))
                .get()
                .isEqualTo(jobResourceRequirements);
    }

    @Test
    void testReadNonExistentResourceRequirementsFromJobGraph() throws IOException {
        assertThat(JobResourceRequirements.readFromJobGraph(JobGraphTestUtils.emptyJobGraph()))
                .isEmpty();
    }
}
