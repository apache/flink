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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.runtime.application.AbstractApplication;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/** {@code TestingApplication} implements {@link AbstractApplication} for testing purposes. */
public class TestingApplication extends AbstractApplication {

    private final Function<ExecuteParams, CompletableFuture<Acknowledge>> executeFunction;
    private final Function<Void, Void> cancelFunction;
    private final Function<Void, ApplicationState> getApplicationStatusFunction;

    private TestingApplication(
            ApplicationID applicationId,
            Function<ExecuteParams, CompletableFuture<Acknowledge>> executeFunction,
            Function<Void, Void> cancelFunction,
            Function<Void, ApplicationState> getApplicationStatusFunction) {
        super(applicationId);
        this.executeFunction = executeFunction;
        this.cancelFunction = cancelFunction;
        this.getApplicationStatusFunction = getApplicationStatusFunction;
    }

    @Override
    public CompletableFuture<Acknowledge> execute(
            DispatcherGateway dispatcherGateway,
            ScheduledExecutor scheduledExecutor,
            Executor mainThreadExecutor,
            FatalErrorHandler errorHandler) {

        ExecuteParams params =
                new ExecuteParams(
                        dispatcherGateway, scheduledExecutor, mainThreadExecutor, errorHandler);
        return executeFunction.apply(params);
    }

    @Override
    public void cancel() {
        cancelFunction.apply(null);
    }

    @Override
    public void dispose() {}

    @Override
    public String getName() {
        return "TestingApplication";
    }

    @Override
    public ApplicationState getApplicationStatus() {
        return getApplicationStatusFunction.apply(null);
    }

    public static class ExecuteParams {
        public final DispatcherGateway dispatcherGateway;
        public final ScheduledExecutor scheduledExecutor;
        public final Executor mainThreadExecutor;
        public final FatalErrorHandler errorHandler;

        public ExecuteParams(
                DispatcherGateway dispatcherGateway,
                ScheduledExecutor scheduledExecutor,
                Executor mainThreadExecutor,
                FatalErrorHandler errorHandler) {
            this.dispatcherGateway = dispatcherGateway;
            this.scheduledExecutor = scheduledExecutor;
            this.mainThreadExecutor = mainThreadExecutor;
            this.errorHandler = errorHandler;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        static final Function<ExecuteParams, CompletableFuture<Acknowledge>>
                DEFAULT_EXECUTE_FUNCTION =
                        params -> CompletableFuture.completedFuture(Acknowledge.get());
        static final Function<Void, Void> DEFAULT_CANCEL_FUNCTION = ignored -> null;
        static final Function<Void, ApplicationState> DEFAULT_GET_APPLICATION_STATUS_FUNCTION =
                ignored -> ApplicationState.RUNNING;

        private ApplicationID applicationId = new ApplicationID();
        private Function<ExecuteParams, CompletableFuture<Acknowledge>> executeFunction =
                DEFAULT_EXECUTE_FUNCTION;
        private Function<Void, Void> cancelFunction = DEFAULT_CANCEL_FUNCTION;
        private Function<Void, ApplicationState> getApplicationStatusFunction =
                DEFAULT_GET_APPLICATION_STATUS_FUNCTION;

        public Builder setApplicationId(ApplicationID applicationId) {
            this.applicationId = applicationId;
            return this;
        }

        public Builder setExecuteFunction(
                Function<ExecuteParams, CompletableFuture<Acknowledge>> executeFunction) {
            this.executeFunction = executeFunction;
            return this;
        }

        public Builder setCancelFunction(Function<Void, Void> cancelFunction) {
            this.cancelFunction = cancelFunction;
            return this;
        }

        public Builder setGetApplicationStatusFunction(
                Function<Void, ApplicationState> getApplicationStatusFunction) {
            this.getApplicationStatusFunction = getApplicationStatusFunction;
            return this;
        }

        public TestingApplication build() {
            return new TestingApplication(
                    applicationId, executeFunction, cancelFunction, getApplicationStatusFunction);
        }
    }
}
