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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.utils.PlannerMocks;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkRelOptClusterFactory}. */
class FlinkRelOptClusterFactoryTest {

    @Test
    void testMetadataQuerySupplierUsableOnFreshThread() throws Throwable {
        PlannerContext plannerContext = PlannerMocks.create().getPlannerContext();
        RelOptCluster cluster = plannerContext.createRelBuilder().getCluster();

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread freshThread =
                new Thread(
                        () -> {
                            try {
                                // Simulate a worker thread that has never had THREAD_PROVIDERS set.
                                RelMetadataQueryBase.THREAD_PROVIDERS.remove();

                                RelMetadataQuery mq = cluster.getMetadataQuerySupplier().get();
                                assertThat(mq.metadataProvider).isNotNull();
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        });
        freshThread.start();
        freshThread.join();

        if (failure.get() != null) {
            throw failure.get();
        }
    }
}
