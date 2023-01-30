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

package org.apache.flink.util;

import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.util.concurrent.ThreadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for uncaught exceptions that will log the exception and kill the process afterwards.
 *
 * <p>This guarantees that critical exceptions are not accidentally lost and leave the system
 * running in an inconsistent state.
 */
public final class FatalExitExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FatalExitExceptionHandler.class);

    public static final FatalExitExceptionHandler INSTANCE = new FatalExitExceptionHandler();
    public static final int EXIT_CODE = -17;

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        try {
            LOG.error(
                    "FATAL: Thread '{}' produced an uncaught exception. Stopping the process...",
                    t.getName(),
                    e);
            ThreadUtils.errorLogThreadDump(LOG);
        } finally {
            FlinkSecurityManager.forceProcessExit(EXIT_CODE);
        }
    }
}
