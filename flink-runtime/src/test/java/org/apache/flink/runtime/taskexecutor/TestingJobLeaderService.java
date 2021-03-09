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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.QuadConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.function.Consumer;
import java.util.function.Function;

/** Testing {@link JobLeaderService} implementation. */
public class TestingJobLeaderService implements JobLeaderService {

    private final QuadConsumer<String, RpcService, HighAvailabilityServices, JobLeaderListener>
            startConsumer;
    private final ThrowingRunnable<? extends Exception> stopRunnable;
    private final Consumer<JobID> removeJobConsumer;
    private final BiConsumerWithException<JobID, String, ? extends Exception> addJobConsumer;
    private final Consumer<JobID> reconnectConsumer;
    private final Function<JobID, Boolean> containsJobFunction;

    TestingJobLeaderService(
            QuadConsumer<String, RpcService, HighAvailabilityServices, JobLeaderListener>
                    startConsumer,
            ThrowingRunnable<? extends Exception> stopRunnable,
            Consumer<JobID> removeJobConsumer,
            BiConsumerWithException<JobID, String, ? extends Exception> addJobConsumer,
            Consumer<JobID> reconnectConsumer,
            Function<JobID, Boolean> containsJobFunction) {
        this.startConsumer = startConsumer;
        this.stopRunnable = stopRunnable;
        this.removeJobConsumer = removeJobConsumer;
        this.addJobConsumer = addJobConsumer;
        this.reconnectConsumer = reconnectConsumer;
        this.containsJobFunction = containsJobFunction;
    }

    @Override
    public void start(
            String initialOwnerAddress,
            RpcService initialRpcService,
            HighAvailabilityServices initialHighAvailabilityServices,
            JobLeaderListener initialJobLeaderListener) {
        startConsumer.accept(
                initialOwnerAddress,
                initialRpcService,
                initialHighAvailabilityServices,
                initialJobLeaderListener);
    }

    @Override
    public void stop() throws Exception {
        stopRunnable.run();
    }

    @Override
    public void removeJob(JobID jobId) {
        removeJobConsumer.accept(jobId);
    }

    @Override
    public void addJob(JobID jobId, String defaultTargetAddress) throws Exception {
        addJobConsumer.accept(jobId, defaultTargetAddress);
    }

    @Override
    public void reconnect(JobID jobId) {
        reconnectConsumer.accept(jobId);
    }

    @Override
    public boolean containsJob(JobID jobId) {
        return containsJobFunction.apply(jobId);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private QuadConsumer<String, RpcService, HighAvailabilityServices, JobLeaderListener>
                startConsumer = (ignoredA, ignoredB, ignoredC, ignoredD) -> {};
        private ThrowingRunnable<? extends Exception> stopRunnable = () -> {};
        private Consumer<JobID> removeJobConsumer = (ignored) -> {};
        private BiConsumerWithException<JobID, String, ? extends Exception> addJobConsumer =
                (ignoredA, ignoredB) -> {};
        private Consumer<JobID> reconnectConsumer = (ignored) -> {};
        private Function<JobID, Boolean> containsJobFunction = (ignored) -> false;

        private Builder() {}

        public Builder setStartConsumer(
                QuadConsumer<String, RpcService, HighAvailabilityServices, JobLeaderListener>
                        startConsumer) {
            this.startConsumer = startConsumer;
            return this;
        }

        public Builder setStopRunnable(ThrowingRunnable<? extends Exception> stopRunnable) {
            this.stopRunnable = stopRunnable;
            return this;
        }

        public Builder setRemoveJobConsumer(Consumer<JobID> removeJobConsumer) {
            this.removeJobConsumer = removeJobConsumer;
            return this;
        }

        public Builder setAddJobConsumer(
                BiConsumerWithException<JobID, String, ? extends Exception> addJobConsumer) {
            this.addJobConsumer = addJobConsumer;
            return this;
        }

        public Builder setReconnectConsumer(Consumer<JobID> reconnectConsumer) {
            this.reconnectConsumer = reconnectConsumer;
            return this;
        }

        public Builder setContainsJobFunction(Function<JobID, Boolean> containsJobFunction) {
            this.containsJobFunction = containsJobFunction;
            return this;
        }

        public TestingJobLeaderService build() {
            return new TestingJobLeaderService(
                    startConsumer,
                    stopRunnable,
                    removeJobConsumer,
                    addJobConsumer,
                    reconnectConsumer,
                    containsJobFunction);
        }
    }
}
