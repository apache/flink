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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Reference;

import javax.annotation.Nullable;

import java.io.File;
import java.net.InetAddress;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration for the task manager services such as the memory manager, the io manager and the
 * metric registry.
 */
public class TaskManagerServicesConfiguration {

    private static final String LOCAL_STATE_SUB_DIRECTORY_ROOT = "localState_";

    private final Configuration configuration;

    private final ResourceID resourceID;

    private final String externalAddress;

    private final InetAddress bindAddress;

    private final int externalDataPort;

    private final boolean localCommunicationOnly;

    private final String[] tmpDirPaths;

    private final Reference<File[]> localRecoveryStateDirectories;

    private final int numberOfSlots;

    @Nullable private final QueryableStateConfiguration queryableStateConfig;

    private final int pageSize;

    private final long timerServiceShutdownTimeout;

    private final boolean localRecoveryEnabled;

    private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

    private Optional<Time> systemResourceMetricsProbingInterval;

    private final TaskExecutorResourceSpec taskExecutorResourceSpec;

    private final FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder;

    private final String[] alwaysParentFirstLoaderPatterns;

    private final int numIoThreads;

    private TaskManagerServicesConfiguration(
            Configuration configuration,
            ResourceID resourceID,
            String externalAddress,
            InetAddress bindAddress,
            int externalDataPort,
            boolean localCommunicationOnly,
            String[] tmpDirPaths,
            Reference<File[]> localRecoveryStateDirectories,
            boolean localRecoveryEnabled,
            @Nullable QueryableStateConfiguration queryableStateConfig,
            int numberOfSlots,
            int pageSize,
            TaskExecutorResourceSpec taskExecutorResourceSpec,
            long timerServiceShutdownTimeout,
            RetryingRegistrationConfiguration retryingRegistrationConfiguration,
            Optional<Time> systemResourceMetricsProbingInterval,
            FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder,
            String[] alwaysParentFirstLoaderPatterns,
            int numIoThreads) {
        this.configuration = checkNotNull(configuration);
        this.resourceID = checkNotNull(resourceID);

        this.externalAddress = checkNotNull(externalAddress);
        this.bindAddress = checkNotNull(bindAddress);
        this.externalDataPort = externalDataPort;
        this.localCommunicationOnly = localCommunicationOnly;
        this.tmpDirPaths = checkNotNull(tmpDirPaths);
        this.localRecoveryStateDirectories = checkNotNull(localRecoveryStateDirectories);
        this.localRecoveryEnabled = checkNotNull(localRecoveryEnabled);
        this.queryableStateConfig = queryableStateConfig;
        this.numberOfSlots = checkNotNull(numberOfSlots);

        this.pageSize = pageSize;

        this.taskExecutorResourceSpec = taskExecutorResourceSpec;
        this.classLoaderResolveOrder = classLoaderResolveOrder;
        this.alwaysParentFirstLoaderPatterns = alwaysParentFirstLoaderPatterns;
        this.numIoThreads = numIoThreads;

        checkArgument(
                timerServiceShutdownTimeout >= 0L,
                "The timer " + "service shutdown timeout must be greater or equal to 0.");
        this.timerServiceShutdownTimeout = timerServiceShutdownTimeout;
        this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);

        this.systemResourceMetricsProbingInterval =
                checkNotNull(systemResourceMetricsProbingInterval);
    }

    // --------------------------------------------------------------------------------------------
    //  Getter/Setter
    // --------------------------------------------------------------------------------------------

    public Configuration getConfiguration() {
        return configuration;
    }

    public ResourceID getResourceID() {
        return resourceID;
    }

    String getExternalAddress() {
        return externalAddress;
    }

    InetAddress getBindAddress() {
        return bindAddress;
    }

    int getExternalDataPort() {
        return externalDataPort;
    }

    boolean isLocalCommunicationOnly() {
        return localCommunicationOnly;
    }

    public String[] getTmpDirPaths() {
        return tmpDirPaths;
    }

    Reference<File[]> getLocalRecoveryStateDirectories() {
        return localRecoveryStateDirectories;
    }

    boolean isLocalRecoveryEnabled() {
        return localRecoveryEnabled;
    }

    @Nullable
    QueryableStateConfiguration getQueryableStateConfig() {
        return queryableStateConfig;
    }

    public int getNumberOfSlots() {
        return numberOfSlots;
    }

    public int getPageSize() {
        return pageSize;
    }

    public TaskExecutorResourceSpec getTaskExecutorResourceSpec() {
        return taskExecutorResourceSpec;
    }

    public MemorySize getNetworkMemorySize() {
        return taskExecutorResourceSpec.getNetworkMemSize();
    }

    public MemorySize getManagedMemorySize() {
        return taskExecutorResourceSpec.getManagedMemorySize();
    }

    long getTimerServiceShutdownTimeout() {
        return timerServiceShutdownTimeout;
    }

    public Optional<Time> getSystemResourceMetricsProbingInterval() {
        return systemResourceMetricsProbingInterval;
    }

    RetryingRegistrationConfiguration getRetryingRegistrationConfiguration() {
        return retryingRegistrationConfiguration;
    }

    public FlinkUserCodeClassLoaders.ResolveOrder getClassLoaderResolveOrder() {
        return classLoaderResolveOrder;
    }

    public String[] getAlwaysParentFirstLoaderPatterns() {
        return alwaysParentFirstLoaderPatterns;
    }

    public int getNumIoThreads() {
        return numIoThreads;
    }

    // --------------------------------------------------------------------------------------------
    //  Parsing of Flink configuration
    // --------------------------------------------------------------------------------------------

    /**
     * Utility method to extract TaskManager config parameters from the configuration and to sanity
     * check them.
     *
     * @param configuration The configuration.
     * @param resourceID resource ID of the task manager
     * @param externalAddress identifying the IP address under which the TaskManager will be
     *     accessible
     * @param localCommunicationOnly True if only local communication is possible. Use only in cases
     *     where only one task manager runs.
     * @param taskExecutorResourceSpec resource specification of the TaskManager to start
     * @param workingDirectory working directory of the TaskManager
     * @return configuration of task manager services used to create them
     */
    public static TaskManagerServicesConfiguration fromConfiguration(
            Configuration configuration,
            ResourceID resourceID,
            String externalAddress,
            boolean localCommunicationOnly,
            TaskExecutorResourceSpec taskExecutorResourceSpec,
            WorkingDirectory workingDirectory)
            throws Exception {
        String[] localStateRootDirs = ConfigurationUtils.parseLocalStateDirectories(configuration);
        final Reference<File[]> localStateDirs;

        if (localStateRootDirs.length == 0) {
            localStateDirs =
                    Reference.borrowed(new File[] {workingDirectory.getLocalStateDirectory()});
        } else {
            File[] createdLocalStateDirs = new File[localStateRootDirs.length];
            final String localStateDirectoryName = LOCAL_STATE_SUB_DIRECTORY_ROOT + resourceID;

            for (int i = 0; i < localStateRootDirs.length; i++) {
                createdLocalStateDirs[i] = new File(localStateRootDirs[i], localStateDirectoryName);
            }

            localStateDirs = Reference.owned(createdLocalStateDirs);
        }

        boolean localRecoveryMode = configuration.getBoolean(CheckpointingOptions.LOCAL_RECOVERY);

        final QueryableStateConfiguration queryableStateConfig =
                QueryableStateConfiguration.fromConfiguration(configuration);

        long timerServiceShutdownTimeout =
                configuration.get(AkkaOptions.ASK_TIMEOUT_DURATION).toMillis();

        final RetryingRegistrationConfiguration retryingRegistrationConfiguration =
                RetryingRegistrationConfiguration.fromConfiguration(configuration);

        final int externalDataPort =
                configuration.getInteger(NettyShuffleEnvironmentOptions.DATA_PORT);

        String bindAddr =
                configuration.getString(
                        TaskManagerOptions.BIND_HOST, NetUtils.getWildcardIPAddress());
        InetAddress bindAddress = InetAddress.getByName(bindAddr);

        final String classLoaderResolveOrder =
                configuration.getString(CoreOptions.CLASSLOADER_RESOLVE_ORDER);

        final String[] alwaysParentFirstLoaderPatterns =
                CoreOptions.getParentFirstLoaderPatterns(configuration);

        final int numIoThreads = ClusterEntrypointUtils.getPoolSize(configuration);

        final String[] tmpDirs = ConfigurationUtils.parseTempDirectories(configuration);

        return new TaskManagerServicesConfiguration(
                configuration,
                resourceID,
                externalAddress,
                bindAddress,
                externalDataPort,
                localCommunicationOnly,
                tmpDirs,
                localStateDirs,
                localRecoveryMode,
                queryableStateConfig,
                ConfigurationParserUtils.getSlot(configuration),
                ConfigurationParserUtils.getPageSize(configuration),
                taskExecutorResourceSpec,
                timerServiceShutdownTimeout,
                retryingRegistrationConfiguration,
                ConfigurationUtils.getSystemResourceMetricsProbingInterval(configuration),
                FlinkUserCodeClassLoaders.ResolveOrder.fromString(classLoaderResolveOrder),
                alwaysParentFirstLoaderPatterns,
                numIoThreads);
    }
}
