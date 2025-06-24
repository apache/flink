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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.RunnableWithException;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;

/** The test task that creates an idle (sleeping) thread. */
public class IdleTestTask implements SampleableTask {

    private final ExecutionAttemptID executionAttemptID = createExecutionAttemptId();
    private final Thread thread;
    private volatile boolean stopped = false;

    /** Instantiates a new idle test task with default sleep duration (10s). */
    public IdleTestTask() {
        this(10000L);
    }

    /**
     * Instantiates a new idle test task.
     *
     * @param sleepDuration the sleep duration
     */
    public IdleTestTask(long sleepDuration) {
        CountDownLatch startSignal = new CountDownLatch(1);
        thread =
                new Thread(
                        () -> {
                            while (!stopped) {
                                try {
                                    startSignal.countDown();
                                    Thread.sleep(sleepDuration);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        });
        thread.setDaemon(true);
        thread.start();
        try {
            startSignal.await(); // This ensures that the new test task always has a stack trace.
        } catch (InterruptedException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public Thread getExecutingThread() {
        return thread;
    }

    @Override
    public ExecutionAttemptID getExecutionId() {
        return executionAttemptID;
    }

    public void start() {
        thread.setDaemon(true);
        thread.start();
        stopped = false;
    }

    public void stop() {
        this.stopped = true;
    }

    public static void executeWithTerminationGuarantee(
            RunnableWithException code, Set<IdleTestTask> tasks) throws Exception {
        try {
            code.run();
        } finally {
            for (IdleTestTask task : tasks) {
                task.stop();
            }
        }
    }
}
