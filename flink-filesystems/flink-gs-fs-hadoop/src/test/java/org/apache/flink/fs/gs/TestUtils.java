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

package org.apache.flink.fs.gs;

import java.util.HashMap;
import java.util.Map;

/** Constants for testing purposes. */
public class TestUtils {

    public static final long RANDOM_SEED = 27;

    /**
     * Helper to create a hadoop configuration from a map.
     *
     * @param values Map of values
     * @return Hadoop config
     */
    public static org.apache.hadoop.conf.Configuration hadoopConfigFromMap(
            Map<String, String> values) {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            hadoopConfig.set(entry.getKey(), entry.getValue());
        }
        return hadoopConfig;
    }

    /**
     * Helper to translate Hadoop config to a map.
     *
     * @param hadoopConfig The Hadoop config
     * @return The map of keys/values
     */
    public static Map<String, String> hadoopConfigToMap(
            org.apache.hadoop.conf.Configuration hadoopConfig) {
        HashMap<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> entry : hadoopConfig) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }
}
