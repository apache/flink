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

package org.apache.flink.table.gateway.api.operation;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/** {@link OperationValidator} util class. */
public final class OperationValidatorUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OperationValidatorUtils.class);

    public static Set<OperationValidator> discoverOperationValidators(Configuration conf) {
        Set<OperationValidator> operationValidators = new HashSet<>();
        final String pluginsDir =
                System.getenv()
                        .getOrDefault(
                                ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                                ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS);

        final PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(conf);
        pluginManager
                .load(OperationValidator.class)
                .forEachRemaining(
                        validator -> {
                            validator.configure(conf);
                            operationValidators.add(validator);
                            LOG.info(
                                    "Discovered operation validator: {}, from plugin directory: {}.",
                                    validator.getClass().getName(),
                                    pluginsDir);
                        });

        return operationValidators;
    }

    private OperationValidatorUtils() {}
}
