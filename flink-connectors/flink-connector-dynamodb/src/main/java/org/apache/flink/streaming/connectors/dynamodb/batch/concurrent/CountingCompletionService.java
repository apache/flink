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

package org.apache.flink.streaming.connectors.dynamodb.batch.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Completion service that counts number of tasks in progress. */
public class CountingCompletionService<V> extends ExecutorCompletionService<V> {

    private final AtomicLong submittedTasks = new AtomicLong();
    private final AtomicLong completedTasks = new AtomicLong();

    public CountingCompletionService(Executor executor) {
        super(executor);
    }

    public CountingCompletionService(Executor executor, BlockingQueue<Future<V>> completionQueue) {
        super(executor, completionQueue);
    }

    @Override
    public Future<V> submit(Callable<V> task) {
        Future<V> future = super.submit(task);
        submittedTasks.incrementAndGet();
        return future;
    }

    @Override
    public Future<V> submit(Runnable task, V result) {
        Future<V> future = super.submit(task, result);
        submittedTasks.incrementAndGet();
        return future;
    }

    @Override
    public Future<V> take() throws InterruptedException {
        Future<V> future = super.take();
        completedTasks.incrementAndGet();
        return future;
    }

    @Override
    public Future<V> poll() {
        Future<V> future = super.poll();
        if (future != null) {
            completedTasks.incrementAndGet();
        }
        return future;
    }

    @Override
    public Future<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
        Future<V> future = super.poll(timeout, unit);
        if (future != null) {
            completedTasks.incrementAndGet();
        }
        return future;
    }

    public long getNumberOfTasksInProgress() {
        return submittedTasks.get() - completedTasks.get();
    }
}
