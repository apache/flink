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

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;

import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.annotation.Nullable;

/** Builder for the {@link YarnClusterDescriptor}. */
public final class YarnClusterDescriptorBuilder {

    private final YarnClient yarnClient;
    private final boolean sharedYarnClient;

    private Configuration flinkConfiguration = new Configuration();
    private YarnConfiguration yarnConfiguration = new YarnConfiguration();

    @Nullable private YarnClusterInformationRetriever yarnClusterInformationRetriever = null;

    private YarnClusterDescriptorBuilder(YarnClient yarnClient, boolean sharedYarnClient) {
        this.yarnClient = yarnClient;
        this.sharedYarnClient = sharedYarnClient;
    }

    public YarnClusterDescriptorBuilder setFlinkConfiguration(Configuration flinkConfiguration) {
        this.flinkConfiguration = flinkConfiguration;
        return this;
    }

    public YarnClusterDescriptorBuilder setYarnConfiguration(YarnConfiguration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
        return this;
    }

    public YarnClusterDescriptorBuilder setYarnClusterInformationRetriever(
            YarnClusterInformationRetriever yarnClusterInformationRetriever) {
        this.yarnClusterInformationRetriever = yarnClusterInformationRetriever;
        return this;
    }

    public YarnClusterDescriptor build() {
        final YarnClusterInformationRetriever clusterInformationRetriever;

        if (yarnClusterInformationRetriever == null) {
            clusterInformationRetriever =
                    YarnClientYarnClusterInformationRetriever.create(yarnClient);
        } else {
            clusterInformationRetriever = yarnClusterInformationRetriever;
        }

        return new YarnClusterDescriptor(
                flinkConfiguration,
                yarnConfiguration,
                yarnClient,
                clusterInformationRetriever,
                sharedYarnClient);
    }

    public static YarnClusterDescriptorBuilder newBuilder(
            YarnClient yarnClient, boolean sharedYarnClient) {
        return new YarnClusterDescriptorBuilder(yarnClient, sharedYarnClient);
    }
}
