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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.apache.flink.runtime.util.JobVertexConnectionUtils.connectNewDataSetAsInput;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link NonAdaptiveExecutionHandler}. */
class NonAdaptiveExecutionHandlerTest {

    @Test
    void testGetInitialParallelismAndNotifyJobVertexParallelismDecided() {
        JobVertex v1 = new JobVertex("v1");
        JobVertex v2 = new JobVertex("v2");

        connectNewDataSetAsInput(
                v2, v1, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING, false, true);

        JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder()
                        .addJobVertices(Arrays.asList(v1, v2))
                        .build();
        NonAdaptiveExecutionHandler handler = new NonAdaptiveExecutionHandler(jobGraph);

        assertThat(handler.getInitialParallelism(v1.getID())).isEqualTo(v1.getParallelism());

        Random random = new Random();
        int parallelism = 1 + random.nextInt(8);
        handler.notifyJobVertexParallelismDecided(v1.getID(), parallelism);
        assertThat(handler.getInitialParallelism(v2.getID())).isEqualTo(parallelism);
    }
}
