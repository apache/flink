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

package org.apache.flink.runtime.dispatcher.cleanup;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Executor;

/**
 * {@code TestingApplicationResourceCleanerFactory} for adding custom {@link
 * ApplicationResourceCleaner} creation.
 */
public class TestingApplicationResourceCleanerFactory implements ApplicationResourceCleanerFactory {

    private final Collection<GloballyCleanableApplicationResource> globallyCleanableResources;

    private final Executor cleanupExecutor;

    private TestingApplicationResourceCleanerFactory(
            Collection<GloballyCleanableApplicationResource> globallyCleanableResources,
            Executor cleanupExecutor) {
        this.globallyCleanableResources = globallyCleanableResources;
        this.cleanupExecutor = cleanupExecutor;
    }

    @Override
    public ApplicationResourceCleaner createApplicationResourceCleaner(
            ComponentMainThreadExecutor mainThreadExecutor) {
        return applicationId -> {
            mainThreadExecutor.assertRunningInMainThread();
            Throwable t = null;
            for (GloballyCleanableApplicationResource resource : globallyCleanableResources) {
                try {
                    resource.globalCleanupAsync(applicationId, cleanupExecutor).get();
                } catch (Throwable throwable) {
                    t = ExceptionUtils.firstOrSuppressed(throwable, t);
                }
            }
            return t != null
                    ? FutureUtils.completedExceptionally(t)
                    : FutureUtils.completedVoidFuture();
        };
    }

    public static Builder builder() {
        return new Builder();
    }

    /** {@code Builder} for creating {@code TestingApplicationResourceCleanerFactory} instances. */
    public static class Builder {

        private Collection<GloballyCleanableApplicationResource> globallyCleanableResources =
                new ArrayList<>();

        private Executor cleanupExecutor = Executors.directExecutor();

        public Builder setGloballyCleanableResources(
                Collection<GloballyCleanableApplicationResource> globallyCleanableResources) {
            this.globallyCleanableResources = globallyCleanableResources;
            return this;
        }

        public Builder withGloballyCleanableResource(
                GloballyCleanableApplicationResource globallyCleanableResource) {
            this.globallyCleanableResources.add(globallyCleanableResource);
            return this;
        }

        public Builder setCleanupExecutor(Executor cleanupExecutor) {
            this.cleanupExecutor = cleanupExecutor;
            return this;
        }

        public TestingApplicationResourceCleanerFactory build() {
            return new TestingApplicationResourceCleanerFactory(
                    globallyCleanableResources, cleanupExecutor);
        }
    }
}
