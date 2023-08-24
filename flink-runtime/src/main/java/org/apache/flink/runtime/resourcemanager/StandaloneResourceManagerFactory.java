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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.blocklist.BlocklistUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerImpl;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.util.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.UUID;
import java.util.concurrent.Executor;

/** {@link ResourceManagerFactory} which creates a {@link StandaloneResourceManager}. */
public final class StandaloneResourceManagerFactory extends ResourceManagerFactory<ResourceID> {

    private static final StandaloneResourceManagerFactory INSTANCE =
            new StandaloneResourceManagerFactory();

    private StandaloneResourceManagerFactory() {}

    public static StandaloneResourceManagerFactory getInstance() {
        return INSTANCE;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtils.class);

    @Override
    protected ResourceManager<ResourceID> createResourceManager(
            Configuration configuration,
            ResourceID resourceId,
            RpcService rpcService,
            UUID leaderSessionId,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            FatalErrorHandler fatalErrorHandler,
            ClusterInformation clusterInformation,
            @Nullable String webInterfaceUrl,
            ResourceManagerMetricGroup resourceManagerMetricGroup,
            ResourceManagerRuntimeServices resourceManagerRuntimeServices,
            Executor ioExecutor) {

        final Time standaloneClusterStartupPeriodTime =
                ConfigurationUtils.getStandaloneClusterStartupPeriodTime(configuration);

        return new StandaloneResourceManager(
                rpcService,
                leaderSessionId,
                resourceId,
                heartbeatServices,
                delegationTokenManager,
                resourceManagerRuntimeServices.getSlotManager(),
                ResourceManagerPartitionTrackerImpl::new,
                BlocklistUtils.loadBlocklistHandlerFactory(configuration),
                resourceManagerRuntimeServices.getJobLeaderIdService(),
                clusterInformation,
                fatalErrorHandler,
                resourceManagerMetricGroup,
                standaloneClusterStartupPeriodTime,
                Time.fromDuration(configuration.get(AkkaOptions.ASK_TIMEOUT_DURATION)),
                ioExecutor);
    }

    @Override
    protected ResourceManagerRuntimeServicesConfiguration
            createResourceManagerRuntimeServicesConfiguration(Configuration configuration)
                    throws ConfigurationException {
        return ResourceManagerRuntimeServicesConfiguration.fromConfiguration(
                getConfigurationWithoutMaxResourceIfSet(configuration),
                ArbitraryWorkerResourceSpecFactory.INSTANCE);
    }

    /**
     * Get the configuration for standalone ResourceManager, overwrite invalid configs.
     *
     * @param configuration configuration object
     * @return the configuration for standalone ResourceManager
     */
    @VisibleForTesting
    public static Configuration getConfigurationWithoutMaxResourceIfSet(
            Configuration configuration) {
        final Configuration copiedConfig = new Configuration(configuration);
        removeMaxResourceConfig(copiedConfig);

        return copiedConfig;
    }

    private static void removeMaxResourceConfig(Configuration configuration) {
        // The max slot/cpu/memory limit should not take effect for standalone cluster, we
        // overwrite the
        // configure in case user
        // sets this value by mistake.
        if (configuration.removeConfig(ResourceManagerOptions.MAX_SLOT_NUM)) {
            LOG.warn(
                    "Config option {} will be ignored in standalone mode.",
                    ResourceManagerOptions.MAX_SLOT_NUM.key());
        }

        if (configuration.removeConfig(ResourceManagerOptions.MAX_TOTAL_CPU)) {
            LOG.warn(
                    "Config option {} will be ignored in standalone mode.",
                    ResourceManagerOptions.MAX_TOTAL_CPU.key());
        }

        if (configuration.removeConfig(ResourceManagerOptions.MAX_TOTAL_MEM)) {
            LOG.warn(
                    "Config option {} will be ignored in standalone mode.",
                    ResourceManagerOptions.MAX_TOTAL_MEM.key());
        }
    }
}
