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

package org.apache.flink.runtime.testtasks;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

/**
 * Task which blocks until the (static) {@link #unblock()} method is called and then fails with an
 * exception.
 */
public class FailingBlockingInvokable extends AbstractInvokable {
    private static volatile boolean blocking = true;
    private static final Object lock = new Object();

    /**
     * Create an Invokable task and set its environment.
     *
     * @param environment The environment assigned to this invokable.
     */
    public FailingBlockingInvokable(Environment environment) {
        super(environment);
    }

    @Override
    public void invoke() throws Exception {
        while (blocking) {
            synchronized (lock) {
                lock.wait();
            }
        }
        throw new RuntimeException("This exception is expected.");
    }

    public static void unblock() {
        blocking = false;

        synchronized (lock) {
            lock.notifyAll();
        }
    }
}
