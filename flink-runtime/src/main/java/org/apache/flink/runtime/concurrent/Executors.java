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

package org.apache.flink.runtime.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import scala.concurrent.ExecutionContext;

/**
 * Collection of {@link Executor}, {@link ExecutorService} and {@link ExecutionContext}
 * implementations.
 */
public class Executors {

    /**
     * Return a direct executor. The direct executor directly executes the runnable in the calling
     * thread.
     *
     * @return Direct executor
     */
    public static Executor directExecutor() {
        return DirectExecutorService.INSTANCE;
    }

    /**
     * Return a new direct executor service.
     *
     * <p>The direct executor service directly executes the runnables and the callables in the
     * calling thread.
     *
     * @return New direct executor service
     */
    public static ExecutorService newDirectExecutorService() {
        return new DirectExecutorService();
    }

    /**
     * Return a direct execution context. The direct execution context executes the runnable
     * directly in the calling thread.
     *
     * @return Direct execution context.
     */
    public static ExecutionContext directExecutionContext() {
        return DirectExecutionContext.INSTANCE;
    }

    /** Direct execution context. */
    private static class DirectExecutionContext implements ExecutionContext {

        static final DirectExecutionContext INSTANCE = new DirectExecutionContext();

        private DirectExecutionContext() {}

        @Override
        public void execute(Runnable runnable) {
            runnable.run();
        }

        @Override
        public void reportFailure(Throwable cause) {
            throw new IllegalStateException("Error in direct execution context.", cause);
        }

        @Override
        public ExecutionContext prepare() {
            return this;
        }
    }
}
