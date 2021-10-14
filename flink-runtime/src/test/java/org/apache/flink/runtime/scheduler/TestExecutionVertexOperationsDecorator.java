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
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link ExecutionVertexOperations} decorator that enables instrumentation of execution vertex
 * operations for testing purposes.
 */
public class TestExecutionVertexOperationsDecorator implements ExecutionVertexOperations {

    private final ExecutionVertexOperations delegate;

    private final CountLatch deployedVertices = new CountLatch();
    private final CountLatch canceledVertices = new CountLatch();
    private final CountLatch failedVertices = new CountLatch();

    private boolean failDeploy;

    public TestExecutionVertexOperationsDecorator(final ExecutionVertexOperations delegate) {
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public void deploy(final ExecutionVertex executionVertex) throws JobException {
        deployedVertices.add(executionVertex.getID());

        if (failDeploy) {
            throw new RuntimeException("Expected");
        }

        delegate.deploy(executionVertex);
    }

    @Override
    public CompletableFuture<?> cancel(final ExecutionVertex executionVertex) {
        canceledVertices.add(executionVertex.getID());
        return delegate.cancel(executionVertex);
    }

    @Override
    public void markFailed(ExecutionVertex executionVertex, Throwable cause) {
        failedVertices.add(executionVertex.getID());
        delegate.markFailed(executionVertex, cause);
    }

    public void enableFailDeploy() {
        failDeploy = true;
    }

    public void disableFailDeploy() {
        failDeploy = false;
    }

    public List<ExecutionVertexID> getDeployedVertices() {
        return deployedVertices.getVertices();
    }

    public List<ExecutionVertexID> getCanceledVertices() {
        return canceledVertices.getVertices();
    }

    public List<ExecutionVertexID> getFailedVertices() {
        return failedVertices.getVertices();
    }

    /** Waits until the given number of vertices have been canceled. */
    public void awaitCanceledVertices(int count) throws InterruptedException {
        canceledVertices.await(count);
    }

    /** Waits until the given number of vertices have been failed. */
    public void awaitFailedVertices(int count) throws InterruptedException {
        failedVertices.await(count);
    }

    private static class CountLatch {
        @GuardedBy("lock")
        private final List<ExecutionVertexID> vertices = new ArrayList<>();

        private final Object lock = new Object();

        public void add(ExecutionVertexID executionVertexId) {
            synchronized (lock) {
                vertices.add(executionVertexId);
                lock.notifyAll();
            }
        }

        public void await(int count) throws InterruptedException {
            synchronized (lock) {
                while (vertices.size() < count) {
                    lock.wait();
                }
            }
        }

        public List<ExecutionVertexID> getVertices() {
            synchronized (lock) {
                return Collections.unmodifiableList(vertices);
            }
        }
    }
}
