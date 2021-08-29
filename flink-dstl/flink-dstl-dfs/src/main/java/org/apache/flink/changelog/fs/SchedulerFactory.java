/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.changelog.fs;

import org.slf4j.Logger;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.FatalExitExceptionHandler.INSTANCE;

class SchedulerFactory {
    private SchedulerFactory() {}

    /**
     * Create a {@link ScheduledThreadPoolExecutor} using the provided corePoolSize. The following
     * behaviour is configured:
     *
     * <ul>
     *   <li>rejected executions are logged if the executor is {@link
     *       java.util.concurrent.ThreadPoolExecutor#isShutdown shutdown}
     *   <li>otherwise, {@link RejectedExecutionException} is thrown
     *   <li>any uncaught exception fails the JVM (using {@link
     *       org.apache.flink.runtime.util.FatalExitExceptionHandler FatalExitExceptionHandler})
     * </ul>
     */
    public static ScheduledThreadPoolExecutor create(int corePoolSize, String name, Logger log) {
        AtomicInteger cnt = new AtomicInteger(0);
        return new ScheduledThreadPoolExecutor(
                corePoolSize,
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName(name + "-" + cnt.incrementAndGet());
                    thread.setUncaughtExceptionHandler(INSTANCE);
                    return thread;
                },
                (ignored, executor) -> {
                    if (executor.isShutdown()) {
                        log.debug("Execution rejected because shutdown is in progress");
                    } else {
                        throw new RejectedExecutionException();
                    }
                });
    }
}
