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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.guava30.com.google.common.collect.MapDifference;

/**
 * If {@link org.apache.flink.configuration.DeploymentOptions#ALLOW_CLIENT_JOB_CONFIGURATIONS} is
 * disabled this error denotes the not allowed configuration.
 */
@Internal
public class ConfigurationNotAllowedMessage {

    private ConfigurationNotAllowedMessage() {}

    public static String ofConfigurationKeyAndValue(String configkey, String configValue) {
        return String.format("Configuration %s:%s not allowed.", configkey, configValue);
    }

    public static String ofConfigurationRemoved(String configkey, String configValue) {
        return String.format("Configuration %s:%s was removed.", configkey, configValue);
    }

    public static String ofConfigurationChange(
            String configkey, MapDifference.ValueDifference<String> change) {
        return String.format(
                "Configuration %s was changed from %s to %s.",
                configkey, change.leftValue(), change.rightValue());
    }

    public static String ofConfigurationObject(String configurationObject) {
        return String.format("Configuration object %s changed.", configurationObject);
    }
}
