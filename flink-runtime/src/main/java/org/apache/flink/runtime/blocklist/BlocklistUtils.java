/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.blocklist;

import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SlowTaskDetectorOptions;

/** Utility class for blocklist. */
public class BlocklistUtils {

    public static BlocklistHandler.Factory loadBlocklistHandlerFactory(
            Configuration configuration) {
        if (isBlocklistEnabled(configuration)) {
            return new DefaultBlocklistHandler.Factory(
                    configuration.get(SlowTaskDetectorOptions.CHECK_INTERVAL));
        } else {
            return new NoOpBlocklistHandler.Factory();
        }
    }

    public static boolean isBlocklistEnabled(Configuration configuration) {
        // Currently, only enable blocklist for speculative execution
        return configuration.getBoolean(BatchExecutionOptions.SPECULATIVE_ENABLED);
    }

    /** Private default constructor to avoid being instantiated. */
    private BlocklistUtils() {}
}
