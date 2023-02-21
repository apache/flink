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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for handling any uncaught exceptions
 *
 * <p>Handles any uncaught exceptions according to cluster configuration in {@link ClusterOptions}
 * to either just log exception, or fail job.
 */
public class ClusterUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger LOG =
            LoggerFactory.getLogger(ClusterUncaughtExceptionHandler.class);
    private final ClusterOptions.UncaughtExceptionHandleMode handleMode;

    public ClusterUncaughtExceptionHandler(ClusterOptions.UncaughtExceptionHandleMode handleMode) {
        this.handleMode = handleMode;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        if (handleMode == ClusterOptions.UncaughtExceptionHandleMode.LOG) {
            LOG.error(
                    "WARNING: Thread '{}' produced an uncaught exception. If you want to fail on uncaught exceptions, then configure {} accordingly",
                    t.getName(),
                    ClusterOptions.UNCAUGHT_EXCEPTION_HANDLING.key(),
                    e);
        } else { // by default, fail the job
            FatalExitExceptionHandler.INSTANCE.uncaughtException(t, e);
        }
    }
}
