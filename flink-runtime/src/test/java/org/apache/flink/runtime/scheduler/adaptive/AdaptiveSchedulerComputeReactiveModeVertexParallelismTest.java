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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM;

/** Test vertex parallelism configuration for the {@link AdaptiveScheduler} in Reactive mode. */
@RunWith(Parameterized.class)
public class AdaptiveSchedulerComputeReactiveModeVertexParallelismTest extends TestLogger {
    @Parameterized.Parameters(
            name =
                    "parallelism = {0}, maxParallelism = {1}, expected max = {2}, rescale to = {3}, can rescale = {4}")
    public static Object[][] data() {
        return new Object[][] {
            // default minimum and rescale to higher
            {1, JobVertex.MAX_PARALLELISM_DEFAULT, 128, 129, true},
            // test round up part 1 and rescale to lower
            {171, JobVertex.MAX_PARALLELISM_DEFAULT, 256, 42, false},
            // test round up part 2 and rescale to equal
            {172, JobVertex.MAX_PARALLELISM_DEFAULT, 512, 512, true},
            // test round up limit and rescale to equal
            {
                UPPER_BOUND_MAX_PARALLELISM,
                JobVertex.MAX_PARALLELISM_DEFAULT,
                UPPER_BOUND_MAX_PARALLELISM,
                UPPER_BOUND_MAX_PARALLELISM,
                true
            },
            // test configured / takes precedence computed default and rescale to lower
            {4, UPPER_BOUND_MAX_PARALLELISM, UPPER_BOUND_MAX_PARALLELISM, 3, false},
            // test override takes precedence test configured 2 and rescale to higher
            {4, 7, 7, UPPER_BOUND_MAX_PARALLELISM, true},
        };
    }

    @Parameterized.Parameter(0)
    public int parallelism;

    @Parameterized.Parameter(1)
    public int maxParallelism;

    @Parameterized.Parameter(2)
    public int expectedMaxParallelism;

    @Parameterized.Parameter(3)
    public int maxToScaleTo;

    @Parameterized.Parameter(4)
    public boolean expectedCanRescaleTo;

    @Test
    public void testCreateStoreWithoutAdjustedParallelism() {
        JobVertex jobVertex = createNoOpVertex("test", parallelism, maxParallelism);
        VertexParallelismStore store =
                AdaptiveScheduler.computeReactiveModeVertexParallelismStore(
                        Collections.singleton(jobVertex),
                        SchedulerBase::getDefaultMaxParallelism,
                        false);

        VertexParallelismInformation info = store.getParallelismInfo(jobVertex.getID());

        Assert.assertEquals("parallelism is not adjusted", parallelism, info.getParallelism());
        Assert.assertEquals("expected max", expectedMaxParallelism, info.getMaxParallelism());

        Assert.assertEquals(
                "can rescale max",
                expectedCanRescaleTo,
                info.canRescaleMaxParallelism(maxToScaleTo));
    }

    @Test
    public void testCreateStoreWithAdjustedParallelism() {
        JobVertex jobVertex = createNoOpVertex("test", parallelism, maxParallelism);
        VertexParallelismStore store =
                AdaptiveScheduler.computeReactiveModeVertexParallelismStore(
                        Collections.singleton(jobVertex),
                        SchedulerBase::getDefaultMaxParallelism,
                        true);

        VertexParallelismInformation info = store.getParallelismInfo(jobVertex.getID());

        Assert.assertEquals(
                "parallelism is adjusted to max", expectedMaxParallelism, info.getParallelism());
        Assert.assertEquals("expected max", expectedMaxParallelism, info.getMaxParallelism());

        Assert.assertEquals(
                "can rescale max",
                expectedCanRescaleTo,
                info.canRescaleMaxParallelism(maxToScaleTo));
    }
}
