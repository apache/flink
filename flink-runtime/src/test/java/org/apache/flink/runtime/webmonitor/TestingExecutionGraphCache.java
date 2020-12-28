/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.IntSupplier;

/** Testing implementation of {@link ExecutionGraphCache}. */
public class TestingExecutionGraphCache implements ExecutionGraphCache {
    private final IntSupplier sizeSupplier;

    private final BiFunction<JobID, RestfulGateway, CompletableFuture<AccessExecutionGraph>>
            getExecutionGraphFunction;

    private final Runnable cleanupRunnable;

    private final Runnable closeRunnable;

    private TestingExecutionGraphCache(
            IntSupplier sizeSupplier,
            BiFunction<JobID, RestfulGateway, CompletableFuture<AccessExecutionGraph>>
                    getExecutionGraphFunction,
            Runnable cleanupRunnable,
            Runnable closeRunnable) {
        this.sizeSupplier = sizeSupplier;
        this.getExecutionGraphFunction = getExecutionGraphFunction;
        this.cleanupRunnable = cleanupRunnable;
        this.closeRunnable = closeRunnable;
    }

    @Override
    public int size() {
        return sizeSupplier.getAsInt();
    }

    @Override
    public CompletableFuture<AccessExecutionGraph> getExecutionGraph(
            JobID jobId, RestfulGateway restfulGateway) {
        return getExecutionGraphFunction.apply(jobId, restfulGateway);
    }

    @Override
    public void cleanup() {
        cleanupRunnable.run();
    }

    @Override
    public void close() {
        closeRunnable.run();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for the {@link TestingExecutionGraphCache}. */
    public static final class Builder {

        private IntSupplier sizeSupplier = () -> 0;
        private BiFunction<JobID, RestfulGateway, CompletableFuture<AccessExecutionGraph>>
                getExecutionGraphFunction =
                        (ignoredA, ignoredB) ->
                                FutureUtils.completedExceptionally(
                                        new UnsupportedOperationException());
        private Runnable cleanupRunnable = () -> {};
        private Runnable closeRunnable = () -> {};

        private Builder() {}

        public Builder setSizeSupplier(IntSupplier sizeSupplier) {
            this.sizeSupplier = sizeSupplier;
            return this;
        }

        public Builder setGetExecutionGraphFunction(
                BiFunction<JobID, RestfulGateway, CompletableFuture<AccessExecutionGraph>>
                        getExecutionGraphFunction) {
            this.getExecutionGraphFunction = getExecutionGraphFunction;
            return this;
        }

        public Builder setCleanupRunnable(Runnable cleanupRunnable) {
            this.cleanupRunnable = cleanupRunnable;
            return this;
        }

        public Builder setCloseRunnable(Runnable closeRunnable) {
            this.closeRunnable = closeRunnable;
            return this;
        }

        public TestingExecutionGraphCache build() {
            return new TestingExecutionGraphCache(
                    sizeSupplier, getExecutionGraphFunction, cleanupRunnable, closeRunnable);
        }
    }
}
