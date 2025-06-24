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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class AllVerticesIteratorTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testAllVertices() {
        try {

            JobVertex v1 = new JobVertex("v1");
            JobVertex v2 = new JobVertex("v2");
            JobVertex v3 = new JobVertex("v3");
            JobVertex v4 = new JobVertex("v4");

            v1.setInvokableClass(AbstractInvokable.class);
            v2.setInvokableClass(AbstractInvokable.class);
            v3.setInvokableClass(AbstractInvokable.class);
            v4.setInvokableClass(AbstractInvokable.class);

            v1.setParallelism(1);
            v2.setParallelism(7);
            v3.setParallelism(3);
            v4.setParallelism(2);

            ExecutionGraph eg =
                    ExecutionGraphTestUtils.createExecutionGraph(
                            EXECUTOR_RESOURCE.getExecutor(), v1, v2, v3, v4);
            ExecutionJobVertex ejv1 = eg.getJobVertex(v1.getID());
            ExecutionJobVertex ejv2 = eg.getJobVertex(v2.getID());
            ExecutionJobVertex ejv3 = eg.getJobVertex(v3.getID());
            ExecutionJobVertex ejv4 = eg.getJobVertex(v4.getID());

            AllVerticesIterator iter =
                    new AllVerticesIterator(Arrays.asList(ejv1, ejv2, ejv3, ejv4).iterator());

            int numReturned = 0;
            while (iter.hasNext()) {
                iter.hasNext();
                assertThat(iter.next()).isNotNull();
                numReturned++;
            }

            assertThat(numReturned).isEqualTo(13);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
