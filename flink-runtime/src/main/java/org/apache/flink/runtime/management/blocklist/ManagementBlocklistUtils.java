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

package org.apache.flink.runtime.management.blocklist;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ManagementOptions;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.time.Duration;

/** Utility class for management blocklist functionality. */
public class ManagementBlocklistUtils {

    private static final Duration DEFAULT_CLEANUP_INTERVAL = Duration.ofMinutes(1);

    public static ManagementBlocklistHandler.Factory loadManagementBlocklistHandlerFactory(
            Configuration configuration, ScheduledExecutor scheduledExecutor) {
        if (isManagementBlocklistEnabled(configuration)) {
            return new DefaultManagementBlocklistHandler.Factory(
                    scheduledExecutor, DEFAULT_CLEANUP_INTERVAL);
        } else {
            return new NoOpManagementBlocklistHandler.Factory();
        }
    }

    public static boolean isManagementBlocklistEnabled(Configuration configuration) {
        return configuration.get(ManagementOptions.BLOCKLIST_ENABLED);
    }

    public static Duration getDefaultBlocklistDuration(Configuration configuration) {
        return configuration.get(ManagementOptions.BLOCKLIST_DEFAULT_DURATION);
    }

    public static Duration getMaxBlocklistDuration(Configuration configuration) {
        return configuration.get(ManagementOptions.BLOCKLIST_MAX_DURATION);
    }

    /** Private default constructor to avoid being instantiated. */
    private ManagementBlocklistUtils() {}
}
