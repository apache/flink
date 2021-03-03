/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link DefaultVertexAttemptNumberStore}. */
public class DefaultVertexAttemptNumberStoreTest extends TestLogger {
    @Test
    public void testSetAttemptCount() {
        final DefaultVertexAttemptNumberStore vertexAttemptNumberStore =
                new DefaultVertexAttemptNumberStore();

        final JobVertexID jobVertexId = new JobVertexID();
        final int subtaskIndex = 4;
        final int attemptCount = 2;

        vertexAttemptNumberStore.setAttemptCount(jobVertexId, subtaskIndex, attemptCount);
        assertThat(
                vertexAttemptNumberStore
                        .getAttemptCounts(jobVertexId)
                        .getAttemptCount(subtaskIndex),
                is(attemptCount));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetAttemptCountRejectsNegativeSubtaskIndex() {
        final DefaultVertexAttemptNumberStore vertexAttemptNumberStore =
                new DefaultVertexAttemptNumberStore();

        vertexAttemptNumberStore.setAttemptCount(new JobVertexID(), -1, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetAttemptCountRejectsNegativeAttemptCount() {
        final DefaultVertexAttemptNumberStore vertexAttemptNumberStore =
                new DefaultVertexAttemptNumberStore();

        vertexAttemptNumberStore.setAttemptCount(new JobVertexID(), 0, -1);
    }
}
