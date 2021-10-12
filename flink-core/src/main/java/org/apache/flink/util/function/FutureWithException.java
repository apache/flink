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

package org.apache.flink.util.function;

import org.apache.flink.annotation.Internal;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.function.FunctionUtils.asCallable;

/**
 * A {@link Future} that also implements {@link RunnableWithException}. The {@link #run()} is
 * different from {@link FutureTask#run()}, because it throws an exception instead of setting it as
 * an internal state. It is to respect the contract of {@link RunnableWithException}.
 */
@Internal
public class FutureWithException<V> implements RunnableWithException, Future<V> {

    private final FutureTask<V> futureTask;

    public FutureWithException(Callable<V> callable) {
        this.futureTask = new FutureTask<>(callable);
    }

    public FutureWithException(RunnableWithException command) {
        this.futureTask = new FutureTask<>(asCallable(command, null));
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return this.futureTask.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return this.futureTask.isCancelled();
    }

    @Override
    public boolean isDone() {
        return this.futureTask.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        return this.futureTask.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return this.futureTask.get(timeout, unit);
    }

    @Override
    public void run() throws Exception {
        this.futureTask.run();
        this.futureTask.get();
    }
}
