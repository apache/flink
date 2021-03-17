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
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link ExecutionVertexVersioner}. */
public class ExecutionVertexVersionerTest extends TestLogger {

    private static final ExecutionVertexID TEST_EXECUTION_VERTEX_ID1 =
            new ExecutionVertexID(new JobVertexID(), 0);
    private static final ExecutionVertexID TEST_EXECUTION_VERTEX_ID2 =
            new ExecutionVertexID(new JobVertexID(), 0);
    private static final Collection<ExecutionVertexID> TEST_ALL_EXECUTION_VERTEX_IDS =
            Arrays.asList(TEST_EXECUTION_VERTEX_ID1, TEST_EXECUTION_VERTEX_ID2);

    private ExecutionVertexVersioner executionVertexVersioner;

    @Before
    public void setUp() {
        executionVertexVersioner = new ExecutionVertexVersioner();
    }

    @Test
    public void isModifiedReturnsFalseIfVertexUnmodified() {
        final ExecutionVertexVersion executionVertexVersion =
                executionVertexVersioner.recordModification(TEST_EXECUTION_VERTEX_ID1);
        assertFalse(executionVertexVersioner.isModified(executionVertexVersion));
    }

    @Test
    public void isModifiedReturnsTrueIfVertexIsModified() {
        final ExecutionVertexVersion executionVertexVersion =
                executionVertexVersioner.recordModification(TEST_EXECUTION_VERTEX_ID1);
        executionVertexVersioner.recordModification(TEST_EXECUTION_VERTEX_ID1);
        assertTrue(executionVertexVersioner.isModified(executionVertexVersion));
    }

    @Test
    public void throwsExceptionIfVertexWasNeverModified() {
        try {
            executionVertexVersioner.isModified(
                    new ExecutionVertexVersion(TEST_EXECUTION_VERTEX_ID1, 0));
            fail("Expected exception not thrown");
        } catch (final IllegalStateException e) {
            assertThat(
                    e.getMessage(),
                    containsString(
                            "Execution vertex "
                                    + TEST_EXECUTION_VERTEX_ID1
                                    + " does not have a recorded version"));
        }
    }

    @Test
    public void getUnmodifiedVerticesAllVerticesModified() {
        final Set<ExecutionVertexVersion> executionVertexVersions =
                new HashSet<>(
                        executionVertexVersioner
                                .recordVertexModifications(TEST_ALL_EXECUTION_VERTEX_IDS)
                                .values());
        executionVertexVersioner.recordVertexModifications(TEST_ALL_EXECUTION_VERTEX_IDS);

        final Set<ExecutionVertexID> unmodifiedExecutionVertices =
                executionVertexVersioner.getUnmodifiedExecutionVertices(executionVertexVersions);

        assertThat(unmodifiedExecutionVertices, is(empty()));
    }

    @Test
    public void getUnmodifiedVerticesNoVertexModified() {
        final Set<ExecutionVertexVersion> executionVertexVersions =
                new HashSet<>(
                        executionVertexVersioner
                                .recordVertexModifications(TEST_ALL_EXECUTION_VERTEX_IDS)
                                .values());

        final Set<ExecutionVertexID> unmodifiedExecutionVertices =
                executionVertexVersioner.getUnmodifiedExecutionVertices(executionVertexVersions);

        assertThat(
                unmodifiedExecutionVertices,
                containsInAnyOrder(TEST_EXECUTION_VERTEX_ID1, TEST_EXECUTION_VERTEX_ID2));
    }

    @Test
    public void getUnmodifiedVerticesPartOfVerticesModified() {
        final Set<ExecutionVertexVersion> executionVertexVersions =
                new HashSet<>(
                        executionVertexVersioner
                                .recordVertexModifications(TEST_ALL_EXECUTION_VERTEX_IDS)
                                .values());
        executionVertexVersioner.recordModification(TEST_EXECUTION_VERTEX_ID1);

        final Set<ExecutionVertexID> unmodifiedExecutionVertices =
                executionVertexVersioner.getUnmodifiedExecutionVertices(executionVertexVersions);

        assertThat(unmodifiedExecutionVertices, containsInAnyOrder(TEST_EXECUTION_VERTEX_ID2));
    }
}
