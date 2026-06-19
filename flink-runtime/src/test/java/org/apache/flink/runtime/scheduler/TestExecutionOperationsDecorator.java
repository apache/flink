/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link ExecutionOperations} decorator that enables instrumentation of execution operations for
 * testing purposes.
 */
public class TestExecutionOperationsDecorator implements ExecutionOperations {

    private final ExecutionOperations delegate;

    private final CountLatch deployedExecutions = new CountLatch();
    private final CountLatch canceledExecutions = new CountLatch();
    private final CountLatch failedExecutions = new CountLatch();

    private boolean failDeploy;

    public TestExecutionOperationsDecorator(final ExecutionOperations delegate) {
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public void deploy(final Execution execution) throws JobException {
        deployedExecutions.add(execution);
        if (failDeploy) {
            throw new RuntimeException("Expected");
        }
        delegate.deploy(execution);
    }

    @Override
    public CompletableFuture<?> cancel(final Execution execution) {
        canceledExecutions.add(execution);
        return delegate.cancel(execution);
    }

    @Override
    public void markFailed(Execution execution, Throwable cause) {
        failedExecutions.add(execution);
        delegate.markFailed(execution, cause);
    }

    public void enableFailDeploy() {
        failDeploy = true;
    }

    public void disableFailDeploy() {
        failDeploy = false;
    }

    public List<ExecutionAttemptID> getDeployedExecutions() {
        return deployedExecutions.getExecutions();
    }

    public List<ExecutionAttemptID> getCanceledExecutions() {
        return canceledExecutions.getExecutions();
    }

    public List<ExecutionAttemptID> getFailedExecutions() {
        return failedExecutions.getExecutions();
    }

    public List<ExecutionVertexID> getDeployedVertices() {
        return deployedExecutions.getVertices();
    }

    public List<ExecutionVertexID> getCanceledVertices() {
        return canceledExecutions.getVertices();
    }

    public List<ExecutionVertexID> getFailedVertices() {
        return failedExecutions.getVertices();
    }

    /** Waits until the given number of executions have been canceled. */
    public void awaitCanceledExecutions(int count) throws InterruptedException {
        canceledExecutions.await(count);
    }

    /** Waits until the given number of executions have been failed. */
    public void awaitFailedExecutions(int count) throws InterruptedException {
        failedExecutions.await(count);
    }

    private static class CountLatch {
        @GuardedBy("lock")
        private final List<Execution> executions = new ArrayList<>();

        private final Object lock = new Object();

        public void add(Execution execution) {
            synchronized (lock) {
                executions.add(execution);
                lock.notifyAll();
            }
        }

        public void await(int count) throws InterruptedException {
            synchronized (lock) {
                while (executions.size() < count) {
                    lock.wait();
                }
            }
        }

        public List<ExecutionAttemptID> getExecutions() {
            synchronized (lock) {
                return executions.stream()
                        .map(Execution::getAttemptId)
                        .collect(Collectors.toList());
            }
        }

        public List<ExecutionVertexID> getVertices() {
            synchronized (lock) {
                return executions.stream()
                        .map(Execution::getVertex)
                        .map(ExecutionVertex::getID)
                        .collect(Collectors.toList());
            }
        }
    }
}
