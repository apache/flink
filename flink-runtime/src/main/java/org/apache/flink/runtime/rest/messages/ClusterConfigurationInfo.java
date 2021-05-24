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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.rest.handler.cluster.ClusterConfigHandler;

import java.util.ArrayList;
import java.util.Map;

/**
 * Response of the {@link ClusterConfigHandler}, represented as a list of key-value pairs of the
 * cluster {@link Configuration}.
 */
public class ClusterConfigurationInfo extends ArrayList<ClusterConfigurationInfoEntry>
        implements ResponseBody {

    private static final long serialVersionUID = -1170348873871206964L;

    // a default constructor is required for collection type marshalling
    public ClusterConfigurationInfo() {}

    public ClusterConfigurationInfo(int initialEntries) {
        super(initialEntries);
    }

    public static ClusterConfigurationInfo from(Configuration config) {
        final ClusterConfigurationInfo clusterConfig =
                new ClusterConfigurationInfo(config.keySet().size());
        final Map<String, String> configurationWithHiddenSensitiveValues =
                ConfigurationUtils.hideSensitiveValues(config.toMap());

        for (Map.Entry<String, String> keyValuePair :
                configurationWithHiddenSensitiveValues.entrySet()) {
            clusterConfig.add(
                    new ClusterConfigurationInfoEntry(
                            keyValuePair.getKey(), keyValuePair.getValue()));
        }

        return clusterConfig;
    }
}
