/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.configuration;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/** StateChangelog options that are used to pass job-level configuration from JM to TM. */
@Internal
public class StateChangelogOptionsInternal {
    private StateChangelogOptionsInternal() {}

    public static final String CHANGE_LOG_CONFIGURATION = "state.backend.changelog.configuration";

    public static void putConfiguration(
            Configuration target, Configuration changelogConfiguration) {
        try {
            InstantiationUtil.writeObjectToConfig(
                    changelogConfiguration, target, CHANGE_LOG_CONFIGURATION);
        } catch (IOException e) {
            throw new IllegalConfigurationException(
                    "Couldn't serialize changelog configuration", e);
        }
    }

    public static Configuration getConfiguration(
            Configuration jobConfiguration, ClassLoader classLoader)
            throws IllegalConfigurationException {
        try {
            Configuration configuration =
                    InstantiationUtil.readObjectFromConfig(
                            jobConfiguration,
                            StateChangelogOptionsInternal.CHANGE_LOG_CONFIGURATION,
                            classLoader);
            return configuration == null ? new Configuration() : configuration;
        } catch (ClassNotFoundException | IOException e) {
            throw new IllegalConfigurationException(
                    "Couldn't deserialize changelog configuration", e);
        }
    }
}
