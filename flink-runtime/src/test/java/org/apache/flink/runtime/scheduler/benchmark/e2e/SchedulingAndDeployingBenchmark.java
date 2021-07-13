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
 * limitations under the License
 */

package org.apache.flink.runtime.scheduler.benchmark.e2e;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolUtils;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.stream.StreamSupport;

/**
 * The benchmark of scheduling and deploying tasks in a STREAMING/BATCH job. The related method is
 * {@link DefaultScheduler#startScheduling}.
 */
public class SchedulingAndDeployingBenchmark extends SchedulerEndToEndBenchmarkBase {

    private DefaultScheduler scheduler;

    @Override
    public void setup(JobConfiguration jobConfiguration) throws Exception {

        super.setup(jobConfiguration);

        scheduler =
                createScheduler(
                        jobGraph,
                        physicalSlotProvider,
                        mainThreadExecutor,
                        scheduledExecutorService);
    }

    public void startScheduling() throws Exception {
        CompletableFuture.runAsync(
                        () -> {
                            scheduler.startScheduling();

                            final int numberSlots =
                                    StreamSupport.stream(
                                                    jobGraph.getVertices().spliterator(), false)
                                            .mapToInt(JobVertex::getParallelism)
                                            .sum();
                            SlotPoolUtils.tryOfferSlots(
                                    slotPool,
                                    ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                    Collections.nCopies(numberSlots, ResourceProfile.ANY));
                        },
                        mainThreadExecutor)
                .join();
    }
}
