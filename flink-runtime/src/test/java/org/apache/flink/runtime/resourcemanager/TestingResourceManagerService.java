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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/** Implementation of {@link ResourceManagerService} for testing purpose. */
public class TestingResourceManagerService implements ResourceManagerService {

    private final CompletableFuture<Void> terminationFuture;
    private final CompletableFuture<Void> deregisterApplicationFuture;
    private final boolean completeTerminationFutureOnClose;

    private TestingResourceManagerService(
            CompletableFuture<Void> terminationFuture,
            CompletableFuture<Void> deregisterApplicationFuture,
            boolean completeTerminationFutureOnClose) {
        this.terminationFuture = terminationFuture;
        this.deregisterApplicationFuture = deregisterApplicationFuture;
        this.completeTerminationFutureOnClose = completeTerminationFutureOnClose;
    }

    @Override
    public void start() throws Exception {}

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    @Override
    public CompletableFuture<Void> deregisterApplication(
            ApplicationStatus applicationStatus, @Nullable String diagnostics) {
        return deregisterApplicationFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (completeTerminationFutureOnClose) {
            terminationFuture.complete(null);
        }
        return getTerminationFuture();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        private CompletableFuture<Void> deregisterApplicationFuture =
                FutureUtils.completedVoidFuture();
        private boolean completeTerminationFutureOnClose = true;

        public Builder setTerminationFuture(CompletableFuture<Void> terminationFuture) {
            this.terminationFuture = terminationFuture;
            return this;
        }

        public Builder setDeregisterApplicationFuture(
                CompletableFuture<Void> deregisterApplicationFuture) {
            this.deregisterApplicationFuture = deregisterApplicationFuture;
            return this;
        }

        public Builder withManualTerminationFutureCompletion() {
            this.completeTerminationFutureOnClose = false;
            return this;
        }

        public TestingResourceManagerService build() {
            return new TestingResourceManagerService(
                    terminationFuture,
                    deregisterApplicationFuture,
                    completeTerminationFutureOnClose);
        }
    }
}
