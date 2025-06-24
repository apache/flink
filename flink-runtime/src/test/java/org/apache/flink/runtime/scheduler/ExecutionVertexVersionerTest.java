/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ExecutionVertexVersioner}. */
class ExecutionVertexVersionerTest {

    private static final ExecutionVertexID TEST_EXECUTION_VERTEX_ID1 =
            new ExecutionVertexID(new JobVertexID(), 0);
    private static final ExecutionVertexID TEST_EXECUTION_VERTEX_ID2 =
            new ExecutionVertexID(new JobVertexID(), 0);
    private static final Collection<ExecutionVertexID> TEST_ALL_EXECUTION_VERTEX_IDS =
            Arrays.asList(TEST_EXECUTION_VERTEX_ID1, TEST_EXECUTION_VERTEX_ID2);

    private ExecutionVertexVersioner executionVertexVersioner;

    @BeforeEach
    private void setUp() {
        executionVertexVersioner = new ExecutionVertexVersioner();
    }

    @Test
    void isModifiedReturnsFalseIfVertexUnmodified() {
        final ExecutionVertexVersion executionVertexVersion =
                executionVertexVersioner.recordModification(TEST_EXECUTION_VERTEX_ID1);
        assertThat(executionVertexVersioner.isModified(executionVertexVersion)).isFalse();
    }

    @Test
    void isModifiedReturnsTrueIfVertexIsModified() {
        final ExecutionVertexVersion executionVertexVersion =
                executionVertexVersioner.recordModification(TEST_EXECUTION_VERTEX_ID1);
        executionVertexVersioner.recordModification(TEST_EXECUTION_VERTEX_ID1);
        assertThat(executionVertexVersioner.isModified(executionVertexVersion)).isTrue();
    }

    @Test
    void throwsExceptionIfVertexWasNeverModified() {
        assertThatThrownBy(
                        () ->
                                executionVertexVersioner.isModified(
                                        new ExecutionVertexVersion(TEST_EXECUTION_VERTEX_ID1, 0)))
                .withFailMessage("Expected exception not thrown")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Execution vertex "
                                + TEST_EXECUTION_VERTEX_ID1
                                + " does not have a recorded version");
    }

    @Test
    void getUnmodifiedVerticesAllVerticesModified() {
        final Set<ExecutionVertexVersion> executionVertexVersions =
                new HashSet<>(
                        executionVertexVersioner
                                .recordVertexModifications(TEST_ALL_EXECUTION_VERTEX_IDS)
                                .values());
        executionVertexVersioner.recordVertexModifications(TEST_ALL_EXECUTION_VERTEX_IDS);

        final Set<ExecutionVertexID> unmodifiedExecutionVertices =
                executionVertexVersioner.getUnmodifiedExecutionVertices(executionVertexVersions);

        assertThat(unmodifiedExecutionVertices).isEmpty();
    }

    @Test
    void getUnmodifiedVerticesNoVertexModified() {
        final Set<ExecutionVertexVersion> executionVertexVersions =
                new HashSet<>(
                        executionVertexVersioner
                                .recordVertexModifications(TEST_ALL_EXECUTION_VERTEX_IDS)
                                .values());

        final Set<ExecutionVertexID> unmodifiedExecutionVertices =
                executionVertexVersioner.getUnmodifiedExecutionVertices(executionVertexVersions);

        assertThat(unmodifiedExecutionVertices)
                .contains(TEST_EXECUTION_VERTEX_ID1, TEST_EXECUTION_VERTEX_ID2);
    }

    @Test
    void getUnmodifiedVerticesPartOfVerticesModified() {
        final Set<ExecutionVertexVersion> executionVertexVersions =
                new HashSet<>(
                        executionVertexVersioner
                                .recordVertexModifications(TEST_ALL_EXECUTION_VERTEX_IDS)
                                .values());
        executionVertexVersioner.recordModification(TEST_EXECUTION_VERTEX_ID1);

        final Set<ExecutionVertexID> unmodifiedExecutionVertices =
                executionVertexVersioner.getUnmodifiedExecutionVertices(executionVertexVersions);

        assertThat(unmodifiedExecutionVertices).contains(TEST_EXECUTION_VERTEX_ID2);
    }
}
