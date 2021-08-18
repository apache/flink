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
import java.util.concurrent.Future;

/**
 * Mimics a task that is doing something until some external condition is fulfilled. {@link
 * #unblock()} signals that no more work is to be done, unblocking all instances and allowing all
 * future instances to immediately finish as well.
 *
 * <p>The main use-case is keeping a task running while supporting restarts, until some condition is
 * met, at which point it should finish.
 *
 * <p>Before using this class it is important to call {@link #reset}.
 */
public class OnceBlockingNoOpInvokable extends AbstractInvokable {

    private static volatile boolean isBlocking = true;

    private static final Object lock = new Object();

    private static volatile boolean running = true;

    public OnceBlockingNoOpInvokable(Environment environment) {
        super(environment);
    }

    @Override
    public void invoke() throws Exception {
        if (isBlocking) {
            synchronized (lock) {
                while (running) {
                    lock.wait();
                }
            }
        }
    }

    @Override
    public Future<Void> cancel() throws Exception {
        synchronized (lock) {
            running = false;
            lock.notifyAll();
        }
        return CompletableFuture.completedFuture(null);
    }

    public static void unblock() {
        running = false;
        isBlocking = false;
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    public static void reset() {
        isBlocking = true;
        running = true;
    }
}
