/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.slf4j.Logger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utils class for dealing with JVM shutdown hooks. */
public class ShutdownHookUtil {

    /** Adds a shutdown hook to the JVM and returns the Thread, which has been registered. */
    public static Thread addShutdownHook(
            final AutoCloseable service, final String serviceName, final Logger logger) {

        checkNotNull(service);
        checkNotNull(logger);

        final Thread shutdownHook =
                new Thread(
                        () -> {
                            try {
                                service.close();
                            } catch (Throwable t) {
                                logger.error(
                                        "Error during shutdown of {} via JVM shutdown hook.",
                                        serviceName,
                                        t);
                            }
                        },
                        serviceName + " shutdown hook");

        return addShutdownHookThread(shutdownHook, serviceName, logger) ? shutdownHook : null;
    }

    /**
     * Adds a shutdown hook to the JVM.
     *
     * @param shutdownHook Shutdown hook to be registered.
     * @param serviceName The name of service.
     * @param logger The logger to log.
     * @return Whether the hook has been successfully registered.
     */
    public static boolean addShutdownHookThread(
            final Thread shutdownHook, final String serviceName, final Logger logger) {

        checkNotNull(shutdownHook);
        checkNotNull(logger);

        try {
            // Add JVM shutdown hook to call shutdown of service
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            return true;
        } catch (IllegalStateException e) {
            // JVM is already shutting down. no need to do our work
        } catch (Throwable t) {
            logger.error(
                    "Cannot register shutdown hook that cleanly terminates {}.", serviceName, t);
        }
        return false;
    }

    /** Removes a shutdown hook from the JVM. */
    public static void removeShutdownHook(
            final Thread shutdownHook, final String serviceName, final Logger logger) {

        // Do not run if this is invoked by the shutdown hook itself
        if (shutdownHook == null || shutdownHook == Thread.currentThread()) {
            return;
        }

        checkNotNull(logger);

        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException e) {
            // race, JVM is in shutdown already, we can safely ignore this
            logger.debug(
                    "Unable to remove shutdown hook for {}, shutdown already in progress",
                    serviceName,
                    e);
        } catch (Throwable t) {
            logger.warn("Exception while un-registering {}'s shutdown hook.", serviceName, t);
        }
    }

    private ShutdownHookUtil() {
        throw new AssertionError();
    }
}
