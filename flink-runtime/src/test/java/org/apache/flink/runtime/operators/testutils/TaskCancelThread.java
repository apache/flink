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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.util.ExceptionUtils;

import static org.assertj.core.api.Assertions.fail;

public class TaskCancelThread extends Thread {

    private final DriverTestBase<?> cancelDriver;
    private final AbstractInvokable cancelTask;
    private final Thread interruptedThread;

    private final int cancelTimeout;

    public TaskCancelThread(
            int cancelTimeout, Thread interruptedThread, DriverTestBase<?> canceledTask) {
        this.cancelTimeout = cancelTimeout;
        this.interruptedThread = interruptedThread;
        this.cancelDriver = canceledTask;
        this.cancelTask = null;
    }

    public TaskCancelThread(
            int cancelTimeout, Thread interruptedThread, AbstractInvokable canceledTask) {
        this.cancelTimeout = cancelTimeout;
        this.interruptedThread = interruptedThread;
        this.cancelDriver = null;
        this.cancelTask = canceledTask;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(this.cancelTimeout * 1000);
        } catch (InterruptedException e) {
            fail("CancelThread interruped while waiting for cancel timeout");
        }

        try {
            if (this.cancelDriver != null) {
                this.cancelDriver.cancel();
            }
            if (this.cancelTask != null) {
                this.cancelTask.cancel();
            }

            this.interruptedThread.interrupt();
        } catch (Exception e) {
            fail("Canceling task failed: " + ExceptionUtils.stringifyException(e));
        }
    }
}
