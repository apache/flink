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

package org.apache.flink.yarn;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.AbstractContainerizedClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link ClusterClientFactory} for a YARN cluster. */
@Internal
public class YarnClusterClientFactory
        extends AbstractContainerizedClusterClientFactory<ApplicationId> {

    @Override
    public boolean isCompatibleWith(Configuration configuration) {
        checkNotNull(configuration);
        final String deploymentTarget = configuration.get(DeploymentOptions.TARGET);
        return YarnDeploymentTarget.isValidYarnTarget(deploymentTarget);
    }

    @Override
    public YarnClusterDescriptor createClusterDescriptor(Configuration configuration) {
        checkNotNull(configuration);

        final String configurationDirectory = configuration.get(DeploymentOptionsInternal.CONF_DIR);
        YarnLogConfigUtil.setLogConfigFileInConfig(configuration, configurationDirectory);

        return getClusterDescriptor(configuration);
    }

    @Nullable
    @Override
    public ApplicationId getClusterId(Configuration configuration) {
        checkNotNull(configuration);
        final String clusterId = configuration.get(YarnConfigOptions.APPLICATION_ID);
        return clusterId != null ? ApplicationId.fromString(clusterId) : null;
    }

    @Override
    public Optional<String> getApplicationTargetName() {
        return Optional.of(YarnDeploymentTarget.APPLICATION.getName());
    }

    private YarnClusterDescriptor getClusterDescriptor(Configuration configuration) {
        final YarnClient yarnClient = YarnClient.createYarnClient();
        final YarnConfiguration yarnConfiguration =
                Utils.getYarnAndHadoopConfiguration(configuration);

        yarnClient.init(yarnConfiguration);
        yarnClient.start();
        if (configuration.contains(YarnConfigOptions.APPLICATION_NAME)) {
            try {
                Set<String> applicationTypes = Sets.newHashSet();
                applicationTypes.add("Apache Flink");
                EnumSet<YarnApplicationState> applicationStates =
                        EnumSet.noneOf(YarnApplicationState.class);
                applicationStates.add(YarnApplicationState.RUNNING);
                List<ApplicationReport> applicationReports =
                        yarnClient.getApplications(applicationTypes, applicationStates);
                Collections.sort(
                        applicationReports,
                        (o1, o2) -> {
                            long o1Time = o1.getApplicationId().getClusterTimestamp();
                            long o2Time = o2.getApplicationId().getClusterTimestamp();
                            long o1Id = o1.getApplicationId().getId();
                            long o2Id = o2.getApplicationId().getId();
                            if (o1Time > o2Time) {
                                return -1;
                            } else if (o1Time == o2Time) {
                                return (o1Id < o2Id) ? 1 : -1;
                            } else {
                                return 1;
                            }
                        });
                if (CollectionUtils.isNotEmpty(applicationReports)) {
                    for (ApplicationReport applicationReport : applicationReports) {
                        if (StringUtils.equals(
                                applicationReport.getName(),
                                configuration.get(YarnConfigOptions.APPLICATION_NAME))) {
                            if (configuration.contains(YarnConfigOptions.APPLICATION_QUEUE)) {
                                if (StringUtils.equals(
                                        applicationReport.getQueue(),
                                        configuration.get(YarnConfigOptions.APPLICATION_QUEUE))) {
                                    configuration.setString(
                                            YarnConfigOptions.APPLICATION_ID,
                                            applicationReport.getApplicationId().toString());
                                    break;
                                }
                            } else {
                                configuration.setString(
                                        YarnConfigOptions.APPLICATION_ID,
                                        applicationReport.getApplicationId().toString());
                                break;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return new YarnClusterDescriptor(
                configuration,
                yarnConfiguration,
                yarnClient,
                YarnClientYarnClusterInformationRetriever.create(yarnClient),
                false);
    }
}
