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
import org.apache.flink.configuration.DeploymentOptions;

import org.apache.flink.shaded.guava31.com.google.common.collect.MapDifference.ValueDifference;

/**
 * If {@link DeploymentOptions#PROGRAM_CONFIG_ENABLED} is disabled, this error denotes the not
 * allowed configuration.
 */
@Internal
public class ConfigurationNotAllowedMessage {

    private ConfigurationNotAllowedMessage() {}

    public static String ofConfigurationAdded(String configKey, String configValue) {
        return String.format("Configuration %s:%s not allowed.", configKey, configValue);
    }

    public static String ofConfigurationRemoved(String configKey, String configValue) {
        return String.format("Configuration %s:%s was removed.", configKey, configValue);
    }

    public static String ofConfigurationChanged(String configKey, ValueDifference<String> change) {
        return String.format(
                "Configuration %s was changed from %s to %s.",
                configKey, change.leftValue(), change.rightValue());
    }

    public static String ofConfigurationObjectAdded(
            String configurationObject, String configKey, String configValue) {
        return String.format(
                "Configuration %s:%s not allowed in the configuration object %s.",
                configKey, configValue, configurationObject);
    }

    public static String ofConfigurationObjectChanged(
            String configurationObject, String configKey, ValueDifference<String> change) {
        return String.format(
                "Configuration %s was changed from %s to %s in the configuration object %s.",
                configKey, change.leftValue(), change.rightValue(), configurationObject);
    }

    public static String ofConfigurationObjectRemoved(
            String configurationObject, String configKey, String configValue) {
        return String.format(
                "Configuration %s:%s was removed from the configuration object %s.",
                configKey, configValue, configurationObject);
    }

    public static String ofConfigurationObjectSetterUsed(
            String configurationObject, String setter) {
        return String.format("Setter %s#%s has been used", configurationObject, setter);
    }
}
