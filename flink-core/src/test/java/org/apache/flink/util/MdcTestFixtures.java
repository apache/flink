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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MdcOptions;

import java.util.HashMap;
import java.util.Map;

/** Shared test fixtures for MDC-related tests. */
final class MdcTestFixtures {

    /** Returns a two-entry key mapping from generic job config keys to MDC key names. */
    static Map<String, String> testKeyMapping() {
        final Map<String, String> mapping = new HashMap<>();
        mapping.put("job.key-1", "mdc-key-1");
        mapping.put("job.key-2", "mdc-key-2");
        return mapping;
    }

    static Configuration enrichedConfiguration(final String key1Value, final String key2Value) {
        final Configuration conf = new Configuration();
        conf.set(MdcOptions.JOB_CONFIGURATION_TO_MDC_KEYS, testKeyMapping());
        conf.setString("job.key-1", key1Value);
        conf.setString("job.key-2", key2Value);
        return conf;
    }

    static Configuration enrichedConfiguration(final String key1Value) {
        return enrichedConfiguration(key1Value, "val-2");
    }

    private MdcTestFixtures() {}
}
