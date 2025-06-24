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

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;

/** Test vertex parallelism configuration for the {@link SchedulerBase}. */
@ExtendWith(ParameterizedTestExtension.class)
class SchedulerBaseComputeVertexParallelismTest {

    @Parameters(
            name =
                    "parallelism = {0}, maxParallelism = {1}, expected max = {2}, rescale to = {3}, can rescale = {4}")
    private static Collection<Object[]> data() {
        return Arrays.asList(

                // default minimum
                new Object[] {1, JobVertex.MAX_PARALLELISM_DEFAULT, 128, 3, true},
                // test round up part 1
                new Object[] {171, JobVertex.MAX_PARALLELISM_DEFAULT, 256, 42, true},
                // test round up part 2
                new Object[] {172, JobVertex.MAX_PARALLELISM_DEFAULT, 512, 174, true},
                // test round up limit
                new Object[] {
                    UPPER_BOUND_MAX_PARALLELISM,
                    JobVertex.MAX_PARALLELISM_DEFAULT,
                    UPPER_BOUND_MAX_PARALLELISM,
                    UPPER_BOUND_MAX_PARALLELISM,
                    true
                },
                // test configured / takes precedence computed default
                new Object[] {
                    4, UPPER_BOUND_MAX_PARALLELISM, UPPER_BOUND_MAX_PARALLELISM, 3, false
                },
                // test override takes precedence test configured 2
                new Object[] {4, 7, 7, UPPER_BOUND_MAX_PARALLELISM, false});
    }

    @Parameter private int parallelism;

    @Parameter(1)
    private int maxParallelism;

    @Parameter(2)
    private int expectedMaxParallelism;

    @Parameter(3)
    private int maxToScaleTo;

    @Parameter(4)
    private boolean expectedCanRescaleTo;

    @TestTemplate
    void testMaxParallelismDefaulting() {
        JobVertex jobVertex = createNoOpVertex("test", parallelism, maxParallelism);
        VertexParallelismStore store =
                SchedulerBase.computeVertexParallelismStore(Collections.singleton(jobVertex));

        VertexParallelismInformation info = store.getParallelismInfo(jobVertex.getID());

        assertThat(info.getParallelism()).as("constant parallelism").isEqualTo(parallelism);
        assertThat(info.getMaxParallelism()).as("expected max").isEqualTo(expectedMaxParallelism);

        assertThat(info.canRescaleMaxParallelism(maxToScaleTo))
                .as("can rescale max")
                .isEqualTo(expectedCanRescaleTo);
    }
}
