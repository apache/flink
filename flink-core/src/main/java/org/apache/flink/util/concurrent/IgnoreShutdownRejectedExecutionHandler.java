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

package org.apache.flink.util.concurrent;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Rejected executions are ignored or logged in debug if the executor is {@link
 * java.util.concurrent.ThreadPoolExecutor#isShutdown shutdown}. Otherwise, {@link
 * RejectedExecutionException} is thrown.
 */
public class IgnoreShutdownRejectedExecutionHandler implements RejectedExecutionHandler {
    @Nullable private final Logger logger;

    public IgnoreShutdownRejectedExecutionHandler() {
        this(null);
    }

    public IgnoreShutdownRejectedExecutionHandler(@Nullable Logger logger) {
        this.logger = logger;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (executor.isShutdown()) {
            if (logger != null) {
                logger.debug("Execution is rejected because shutdown is in progress");
            }
        } else {
            throw new RejectedExecutionException();
        }
    }
}
