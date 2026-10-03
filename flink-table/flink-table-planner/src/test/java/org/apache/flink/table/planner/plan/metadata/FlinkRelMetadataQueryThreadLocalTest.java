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

import org.apache.flink.table.planner.plan.trait.RelModifiedMonotonicity;

import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that metadata queries survive being issued on a thread other than the one that built the
 * {@link org.apache.calcite.plan.RelOptCluster} (FLINK-36298).
 *
 * <p>The metadata handler provider is a thread-local seeded only on the cluster-building thread, so
 * PyFlink optimizing on a py4j gateway thread hits an unset provider. The test reproduces that
 * invariant deterministically: build the cluster on the main thread, run the metadata query on a
 * fresh worker thread.
 */
class FlinkRelMetadataQueryThreadLocalTest extends FlinkRelMdHandlerTestBase {

    @Test
    void testMetadataQueryOnForeignThread() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<RelModifiedMonotonicity> future =
                    executor.submit(
                            () -> {
                                // Fresh thread with the provider cleared, as with a py4j gateway
                                // thread.
                                RelMetadataQueryBase.THREAD_PROVIDERS.remove();
                                // Rebuild the shared cluster's metadata query on this thread (the
                                // transformTo path).
                                cluster().invalidateMetadataQuery();
                                RelMetadataQuery mq = cluster().getMetadataQuery();
                                return FlinkRelMetadataQuery.reuseOrCreate(mq)
                                        .getRelModifiedMonotonicity(studentLogicalScan());
                            });
            // Before the fix this fails with NullPointerException("metadataHandlerProvider");
            // after the fix the query resolves normally.
            assertThat(future.get()).isNotNull();
        } finally {
            executor.shutdownNow();
        }
    }
}
