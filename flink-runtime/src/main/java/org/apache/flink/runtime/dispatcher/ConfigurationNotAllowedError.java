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

/**
 * If {@link org.apache.flink.configuration.DeploymentOptions#ALLOW_CLIENT_JOB_CONFIGURATIONS} is
 * disabled this error denotes the not allowed configuration.
 */
@Internal
public class ConfigurationNotAllowedError implements JobValidationError {

    private final String errorMessage;

    public ConfigurationNotAllowedError(String configkey, String configValue) {
        this.errorMessage =
                String.format("Configuration %s:%s not allowed.", configkey, configValue);
    }

    public ConfigurationNotAllowedError(String configurationObject) {
        this.errorMessage = String.format("Configuration object %s changed.", configurationObject);
    }

    @Override
    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return "ConfigurationNotAllowedError{" + "errorMessage='" + errorMessage + '\'' + '}';
    }
}
