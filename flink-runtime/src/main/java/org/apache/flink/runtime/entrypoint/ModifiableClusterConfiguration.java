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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;
import java.util.Properties;

/**
 * Configuration class which contains the parsed command line arguments for the {@link
 * ClusterEntrypoint}.
 */
public class ModifiableClusterConfiguration {

    private final boolean flattenConfig;

    private final String configDir;

    private final Properties dynamicProperties;

    private final Properties removeKeyValues;

    private final List<String> removeKeys;

    private final List<Tuple3<String, String, String>> replaceKeyValues;

    public ModifiableClusterConfiguration(
            boolean flattenConfig,
            String configDir,
            Properties dynamicProperties,
            Properties removeKeyValues,
            List<String> removeKeys,
            List<Tuple3<String, String, String>> replaceKeyValues) {
        this.flattenConfig = flattenConfig;
        this.configDir = configDir;
        this.dynamicProperties = dynamicProperties;
        this.removeKeyValues = removeKeyValues;
        this.removeKeys = removeKeys;
        this.replaceKeyValues = replaceKeyValues;
    }

    public boolean flattenConfig() {
        return flattenConfig;
    }

    public Properties getRemoveKeyValues() {
        return removeKeyValues;
    }

    public List<String> getRemoveKeys() {
        return removeKeys;
    }

    public List<Tuple3<String, String, String>> getReplaceKeyValues() {
        return replaceKeyValues;
    }

    public String getConfigDir() {
        return configDir;
    }

    public Properties getDynamicProperties() {
        return dynamicProperties;
    }
}
