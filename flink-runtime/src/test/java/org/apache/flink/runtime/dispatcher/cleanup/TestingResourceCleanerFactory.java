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

/** {@code TestingResourceCleanerFactory} for adding custom {@link ResourceCleaner} creation. */
public class TestingResourceCleanerFactory implements ResourceCleanerFactory {

    private final Collection<LocallyCleanableResource> locallyCleanableResources;
    private final Collection<GloballyCleanableResource> globallyCleanableResources;

    private final Executor cleanupExecutor;

    private TestingResourceCleanerFactory(
            Collection<LocallyCleanableResource> locallyCleanableResources,
            Collection<GloballyCleanableResource> globallyCleanableResources,
            Executor cleanupExecutor) {
        this.locallyCleanableResources = locallyCleanableResources;
        this.globallyCleanableResources = globallyCleanableResources;
        this.cleanupExecutor = cleanupExecutor;
    }

    @Override
    public ResourceCleaner createLocalResourceCleaner(
            ComponentMainThreadExecutor mainThreadExecutor) {
        return createResourceCleaner(
                mainThreadExecutor,
                locallyCleanableResources,
                LocallyCleanableResource::localCleanupAsync);
    }

    @Override
    public ResourceCleaner createGlobalResourceCleaner(
            ComponentMainThreadExecutor mainThreadExecutor) {
        return createResourceCleaner(
                mainThreadExecutor,
                globallyCleanableResources,
                GloballyCleanableResource::globalCleanupAsync);
    }

    private <T> ResourceCleaner createResourceCleaner(
            ComponentMainThreadExecutor mainThreadExecutor,
            Collection<T> resources,
            DefaultResourceCleaner.CleanupFn<T> cleanupFn) {
        return jobId -> {
            mainThreadExecutor.assertRunningInMainThread();
            Throwable t = null;
            for (T resource : resources) {
                try {
                    cleanupFn.cleanupAsync(resource, jobId, cleanupExecutor).get();
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

    /** {@code Builder} for creating {@code TestingResourceCleanerFactory} instances. */
    public static class Builder {

        private Collection<LocallyCleanableResource> locallyCleanableResources = new ArrayList<>();
        private Collection<GloballyCleanableResource> globallyCleanableResources =
                new ArrayList<>();

        private Executor cleanupExecutor = Executors.directExecutor();

        public Builder setLocallyCleanableResources(
                Collection<LocallyCleanableResource> locallyCleanableResources) {
            this.locallyCleanableResources = locallyCleanableResources;
            return this;
        }

        public Builder withLocallyCleanableResource(
                LocallyCleanableResource locallyCleanableResource) {
            this.locallyCleanableResources.add(locallyCleanableResource);
            return this;
        }

        public Builder setGloballyCleanableResources(
                Collection<GloballyCleanableResource> globallyCleanableResources) {
            this.globallyCleanableResources = globallyCleanableResources;
            return this;
        }

        public Builder withGloballyCleanableResource(
                GloballyCleanableResource globallyCleanableResource) {
            this.globallyCleanableResources.add(globallyCleanableResource);
            return this;
        }

        public Builder setCleanupExecutor(Executor cleanupExecutor) {
            this.cleanupExecutor = cleanupExecutor;
            return this;
        }

        public TestingResourceCleanerFactory build() {
            return new TestingResourceCleanerFactory(
                    locallyCleanableResources, globallyCleanableResources, cleanupExecutor);
        }
    }
}
