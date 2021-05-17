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

package org.apache.flink.runtime.testtasks;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple {@link AbstractInvokable} which blocks the first time it is run. Moreover, one can wait
 * until n instances of this invokable are running by calling {@link #waitUntilOpsAreRunning()}.
 *
 * <p>Before using this class it is important to call {@link #resetFor}.
 */
public class OnceBlockingNoOpInvokable extends AbstractInvokable {

    private static final AtomicInteger instanceCount = new AtomicInteger(0);

    private static volatile CountDownLatch numOpsPending = new CountDownLatch(1);

    private static volatile boolean isBlocking = true;

    private final Object lock = new Object();

    private volatile boolean running = true;

    public OnceBlockingNoOpInvokable(Environment environment) {
        super(environment);
    }

    @Override
    public void invoke() throws Exception {

        instanceCount.incrementAndGet();
        numOpsPending.countDown();

        synchronized (lock) {
            while (isBlocking && running) {
                lock.wait();
            }
        }

        isBlocking = false;
    }

    @Override
    public Future<Void> cancel() throws Exception {
        synchronized (lock) {
            running = false;
            lock.notifyAll();
        }
        return CompletableFuture.completedFuture(null);
    }

    public static void waitUntilOpsAreRunning() throws InterruptedException {
        numOpsPending.await();
    }

    public static int getInstanceCount() {
        return instanceCount.get();
    }

    public static void resetInstanceCount() {
        instanceCount.set(0);
    }

    public static void resetFor(int parallelism) {
        numOpsPending = new CountDownLatch(parallelism);
        isBlocking = true;
    }
}
