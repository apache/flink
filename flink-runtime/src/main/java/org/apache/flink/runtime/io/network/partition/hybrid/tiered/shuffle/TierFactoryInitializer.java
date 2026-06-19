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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.DiskTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory.MemoryTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A class responsible for initializing and returning a list of {@link TierFactory} instances based
 * on the provided {@link TieredStorageConfiguration}. It uses different methods to handle different
 * levels of shuffle data persistence.
 */
public class TierFactoryInitializer {

    /**
     * Initializes and returns a list of {@link TierFactory} instances according to the specified
     * {@code TieredStorageConfiguration}'s persistent level. The method selects the appropriate
     * strategy to create and initialize tier factories based on whether the shuffle data should be
     * ephemeral, tied to task manager (TM) levels, or durable.
     *
     * @param configuration The {@link Configuration} used to initialize the factories.
     * @return A list of initialized {@link TierFactory} instances configured according to the given
     *     persistent level.
     * @throws IllegalArgumentException If an unknown persistent level is encountered.
     */
    public static List<TierFactory> initializeTierFactories(Configuration configuration) {
        String externalTierFactoryClass =
                configuration.get(
                        NettyShuffleEnvironmentOptions
                                .NETWORK_HYBRID_SHUFFLE_EXTERNAL_REMOTE_TIER_FACTORY_CLASS_NAME);
        if (externalTierFactoryClass != null) {
            return Collections.singletonList(
                    createExternalTierFactory(configuration, externalTierFactoryClass));
        } else {
            return getEphemeralTierFactories(configuration);
        }
    }

    private static List<TierFactory> getEphemeralTierFactories(Configuration configuration) {
        String externalTierFactoryClass =
                configuration.get(
                        NettyShuffleEnvironmentOptions
                                .NETWORK_HYBRID_SHUFFLE_EXTERNAL_REMOTE_TIER_FACTORY_CLASS_NAME);
        String remoteStoragePath =
                configuration.get(
                        NettyShuffleEnvironmentOptions
                                .NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_PATH);

        List<TierFactory> tierFactories = new ArrayList<>();
        tierFactories.add(createMemoryTierFactory(configuration));
        tierFactories.add(createDiskTierFactory(configuration));
        if (externalTierFactoryClass != null) {
            tierFactories.add(createExternalTierFactory(configuration, externalTierFactoryClass));
        } else if (remoteStoragePath != null) {
            tierFactories.add(createRemoteTierFactory(configuration));
        }
        return tierFactories;
    }

    private static MemoryTierFactory createMemoryTierFactory(Configuration configuration) {
        MemoryTierFactory memoryTierFactory = new MemoryTierFactory();
        memoryTierFactory.setup(configuration);
        return memoryTierFactory;
    }

    private static DiskTierFactory createDiskTierFactory(Configuration configuration) {
        DiskTierFactory diskTierFactory = new DiskTierFactory();
        diskTierFactory.setup(configuration);
        return diskTierFactory;
    }

    private static RemoteTierFactory createRemoteTierFactory(Configuration configuration) {
        RemoteTierFactory remoteTierFactory = new RemoteTierFactory();
        remoteTierFactory.setup(configuration);
        return remoteTierFactory;
    }

    private static TierFactory createExternalTierFactory(
            Configuration configuration, String externalTierFactoryClassName) {
        checkNotNull(externalTierFactoryClassName);
        TierFactory tierFactory = loadTierFactory(externalTierFactoryClassName);
        tierFactory.setup(configuration);
        return tierFactory;
    }

    /** Loads and instantiates a {@link TierFactory} based on the provided class name. */
    private static TierFactory loadTierFactory(String tierFactoryClassName) {
        TierFactory tierFactory = null;
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            tierFactory =
                    InstantiationUtil.instantiate(
                            tierFactoryClassName, TierFactory.class, classLoader);
        } catch (FlinkException e) {
            ExceptionUtils.rethrow(e);
        }
        return tierFactory;
    }
}
