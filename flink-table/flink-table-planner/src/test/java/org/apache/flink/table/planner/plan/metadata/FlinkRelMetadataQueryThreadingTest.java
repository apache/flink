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

package org.apache.flink.table.planner.plan.metadata;

import org.apache.flink.table.planner.utils.PlannerMocks;

import org.apache.calcite.plan.RelOptCluster;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link FlinkRelMetadataQuery} concerning multithreading. */
public class FlinkRelMetadataQueryThreadingTest {
    @Test
    public void testAccessRelMetadataQueryInstanceFromDifferentThreads()
            throws InterruptedException {
        PlannerMocks plannerMocks = PlannerMocks.create(true);
        RelOptCluster relOptCluster = plannerMocks.getPlannerContext().getCluster();
        relOptCluster.getMetadataQuery();

        Future<?> future =
                Executors.newSingleThreadExecutor()
                        .submit(
                                () -> {
                                    FlinkRelMetadataQuery instance =
                                            FlinkRelMetadataQuery.instance();
                                });

        try {
            future.get();
        } catch (ExecutionException e) {
            if (e.getCause().getClass() == NullPointerException.class) {
                fail("NullPointerException thrown", e.getCause());
            }
        }
    }
}
