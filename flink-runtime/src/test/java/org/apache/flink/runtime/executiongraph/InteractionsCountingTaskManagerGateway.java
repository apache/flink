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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

class InteractionsCountingTaskManagerGateway extends SimpleAckingTaskManagerGateway {

    private final AtomicInteger cancelTaskCount = new AtomicInteger(0);

    private final AtomicInteger submitTaskCount = new AtomicInteger(0);

    private CountDownLatch submitLatch;

    public InteractionsCountingTaskManagerGateway() {
        submitLatch = new CountDownLatch(0);
    }

    public InteractionsCountingTaskManagerGateway(final int expectedSubmitCount) {
        this.submitLatch = new CountDownLatch(expectedSubmitCount);
    }

    @Override
    public CompletableFuture<Acknowledge> cancelTask(
            ExecutionAttemptID executionAttemptID, Time timeout) {
        cancelTaskCount.incrementAndGet();
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
        submitTaskCount.incrementAndGet();
        submitLatch.countDown();
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    void resetCounts() {
        cancelTaskCount.set(0);
        submitTaskCount.set(0);
    }

    int getCancelTaskCount() {
        return cancelTaskCount.get();
    }

    int getSubmitTaskCount() {
        return submitTaskCount.get();
    }

    int getInteractionsCount() {
        return cancelTaskCount.get() + submitTaskCount.get();
    }

    void waitUntilAllTasksAreSubmitted() {
        try {
            submitLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
