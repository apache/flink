/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.util.FlinkException;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

class TestingOperatorCoordinatorHandler implements OperatorCoordinatorHandler {
    private boolean disposed = false;

    public boolean isDisposed() {
        return disposed;
    }

    @Override
    public void disposeAllOperatorCoordinators() {
        disposed = true;
    }

    @Override
    public void initializeOperatorCoordinators(ComponentMainThreadExecutor mainThreadExecutor) {
        // No-op.
    }

    @Override
    public void startAllOperatorCoordinators() {
        // No-op.
    }

    @Override
    public void deliverOperatorEventToCoordinator(
            ExecutionAttemptID taskExecutionId, OperatorID operatorId, OperatorEvent evt)
            throws FlinkException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerAndStartNewCoordinators(
            Collection<OperatorCoordinatorHolder> coordinators,
            ComponentMainThreadExecutor mainThreadExecutor) {
        throw new UnsupportedOperationException();
    }
}
