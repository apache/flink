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

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Testing implementation of {@link DispatcherRunner}. */
public class TestingDispatcherRunner implements DispatcherRunner {
    private final CompletableFuture<ApplicationStatus> shutDownFuture;
    private final Supplier<CompletableFuture<Void>> closeAsyncSupplier;

    private TestingDispatcherRunner(
            CompletableFuture<ApplicationStatus> shutDownFuture,
            Supplier<CompletableFuture<Void>> closeAsyncSupplier) {
        this.shutDownFuture = shutDownFuture;
        this.closeAsyncSupplier = closeAsyncSupplier;
    }

    @Override
    public CompletableFuture<ApplicationStatus> getShutDownFuture() {
        return shutDownFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return closeAsyncSupplier.get();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for {@link TestingDispatcherRunner}. */
    public static final class Builder {
        private CompletableFuture<ApplicationStatus> shutDownFuture = new CompletableFuture<>();
        private Supplier<CompletableFuture<Void>> closeAsyncSupplier =
                FutureUtils::completedVoidFuture;

        public Builder setCloseAsyncSupplier(Supplier<CompletableFuture<Void>> closeAsyncSupplier) {
            this.closeAsyncSupplier = closeAsyncSupplier;
            return this;
        }

        public Builder setShutDownFuture(CompletableFuture<ApplicationStatus> shutDownFuture) {
            this.shutDownFuture = shutDownFuture;
            return this;
        }

        public TestingDispatcherRunner build() {
            return new TestingDispatcherRunner(shutDownFuture, closeAsyncSupplier);
        }
    }
}
