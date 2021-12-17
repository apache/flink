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

import org.apache.flink.util.FatalExitExceptionHandler;

/** Utils related to {@link Runnable}. */
public class Runnables {

    /**
     * Asserts that the given {@link Runnable} does not throw exceptions. If the runnable throws
     * exceptions, then it will call the {@link FatalExitExceptionHandler}.
     *
     * @param runnable to assert for no exceptions
     */
    public static void assertNoException(Runnable runnable) {
        withUncaughtExceptionHandler(runnable, FatalExitExceptionHandler.INSTANCE).run();
    }

    /**
     * Guard {@link Runnable} with uncaughtException handler, because {@link
     * java.util.concurrent.ScheduledExecutorService} does not respect the one assigned to executing
     * {@link Thread} instance.
     *
     * @param runnable Runnable future to guard.
     * @param uncaughtExceptionHandler Handler to call in case of uncaught exception.
     * @return Future with handler.
     */
    public static Runnable withUncaughtExceptionHandler(
            Runnable runnable, Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        return () -> {
            try {
                runnable.run();
            } catch (Throwable t) {
                uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t);
            }
        };
    }
}
