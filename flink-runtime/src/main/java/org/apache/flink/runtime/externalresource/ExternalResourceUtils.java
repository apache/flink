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

package org.apache.flink.runtime.externalresource;

import org.apache.flink.api.common.externalresource.ExternalResourceDriver;
import org.apache.flink.api.common.externalresource.ExternalResourceDriverFactory;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Utility class for external resource framework. */
public class ExternalResourceUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ExternalResourceUtils.class);

    private ExternalResourceUtils() {
        throw new UnsupportedOperationException("This class should never be instantiated.");
    }

    /** Get the enabled external resource list from configuration. */
    private static Set<String> getExternalResourceSet(Configuration config) {
        return new HashSet<>(config.get(ExternalResourceOptions.EXTERNAL_RESOURCE_LIST));
    }

    /**
     * Get the external resources map. The key should be used for deployment specific container
     * request, and values should be the amount of that resource.
     *
     * @param config Configurations
     * @param suffix suffix of config option for deployment specific configuration key
     * @return external resources map, map the amount to the configuration key for deployment
     *     specific container request
     */
    public static Map<String, Long> getExternalResources(Configuration config, String suffix) {
        final Set<String> resourceSet = getExternalResourceSet(config);
        LOG.info("Enabled external resources: {}", resourceSet);

        if (resourceSet.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, Long> externalResourceConfigs = new HashMap<>();
        for (String resourceName : resourceSet) {
            final ConfigOption<Long> amountOption =
                    key(ExternalResourceOptions.getAmountConfigOptionForResource(resourceName))
                            .longType()
                            .noDefaultValue();
            final ConfigOption<String> configKeyOption =
                    key(ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(
                                    resourceName, suffix))
                            .stringType()
                            .noDefaultValue();
            final String configKey = config.getString(configKeyOption);
            final Optional<Long> amountOpt = config.getOptional(amountOption);

            if (StringUtils.isNullOrWhitespaceOnly(configKey)) {
                LOG.warn(
                        "Could not find valid {} for {}. Will ignore that resource.",
                        configKeyOption.key(),
                        resourceName);
                continue;
            }
            if (!amountOpt.isPresent()) {
                LOG.warn(
                        "The amount of the {} should be configured. Will ignore that resource.",
                        resourceName);
                continue;
            } else if (amountOpt.get() <= 0) {
                LOG.warn(
                        "The amount of the {} should be positive while finding {}. Will ignore that resource.",
                        amountOpt.get(),
                        resourceName);
                continue;
            }

            if (externalResourceConfigs.put(configKey, amountOpt.get()) != null) {
                LOG.warn(
                        "Duplicate config key {} occurred for external resources, the one named {} with amount {} will overwrite the value.",
                        configKey,
                        resourceName,
                        amountOpt);
            } else {
                LOG.info(
                        "Add external resource config for {} with key {} value {}.",
                        resourceName,
                        configKey,
                        amountOpt);
            }
        }

        return externalResourceConfigs;
    }

    /** Get the map of resource name and amount of all of enabled external resources. */
    public static Map<String, Long> getExternalResourceAmountMap(Configuration config) {
        final Set<String> resourceSet = getExternalResourceSet(config);
        LOG.info("Enabled external resources: {}", resourceSet);

        if (resourceSet.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, Long> externalResourceAmountMap = new HashMap<>();
        for (String resourceName : resourceSet) {
            final ConfigOption<Long> amountOption =
                    key(ExternalResourceOptions.getAmountConfigOptionForResource(resourceName))
                            .longType()
                            .noDefaultValue();
            final Optional<Long> amountOpt = config.getOptional(amountOption);
            if (!amountOpt.isPresent()) {
                LOG.warn(
                        "The amount of the {} should be configured. Will ignore that resource.",
                        resourceName);
            } else if (amountOpt.get() <= 0) {
                LOG.warn(
                        "The amount of the {} should be positive while finding {}. Will ignore that resource.",
                        amountOpt.get(),
                        resourceName);
            } else {
                externalResourceAmountMap.put(resourceName, amountOpt.get());
            }
        }

        return externalResourceAmountMap;
    }

    /**
     * Instantiate the {@link ExternalResourceDriver ExternalResourceDrivers} for all of enabled
     * external resources. {@link ExternalResourceDriver ExternalResourceDrivers} are mapped to its
     * resource name.
     */
    public static Map<String, ExternalResourceDriver> externalResourceDriversFromConfig(
            Configuration config, PluginManager pluginManager) {
        final Set<String> resourceSet = getExternalResourceSet(config);
        LOG.info("Enabled external resources: {}", resourceSet);

        if (resourceSet.isEmpty()) {
            return Collections.emptyMap();
        }

        final Iterator<ExternalResourceDriverFactory> factoryIterator =
                pluginManager.load(ExternalResourceDriverFactory.class);
        final Map<String, ExternalResourceDriverFactory> externalResourceFactories =
                new HashMap<>();
        factoryIterator.forEachRemaining(
                externalResourceDriverFactory ->
                        externalResourceFactories.put(
                                externalResourceDriverFactory.getClass().getName(),
                                externalResourceDriverFactory));

        final Map<String, ExternalResourceDriver> externalResourceDrivers = new HashMap<>();
        for (String resourceName : resourceSet) {
            final ConfigOption<String> driverClassOption =
                    key(ExternalResourceOptions
                                    .getExternalResourceDriverFactoryConfigOptionForResource(
                                            resourceName))
                            .stringType()
                            .noDefaultValue();
            final String driverFactoryClassName = config.getString(driverClassOption);
            if (StringUtils.isNullOrWhitespaceOnly(driverFactoryClassName)) {
                LOG.warn(
                        "Could not find driver class name for {}. Please make sure {} is configured.",
                        resourceName,
                        driverClassOption.key());
                continue;
            }

            ExternalResourceDriverFactory externalResourceDriverFactory =
                    externalResourceFactories.get(driverFactoryClassName);
            if (externalResourceDriverFactory != null) {
                DelegatingConfiguration delegatingConfiguration =
                        new DelegatingConfiguration(
                                config,
                                ExternalResourceOptions
                                        .getExternalResourceParamConfigPrefixForResource(
                                                resourceName));
                try {
                    externalResourceDrivers.put(
                            resourceName,
                            externalResourceDriverFactory.createExternalResourceDriver(
                                    delegatingConfiguration));
                    LOG.info("Add external resources driver for {}.", resourceName);
                } catch (Exception e) {
                    LOG.warn(
                            "Could not instantiate driver with factory {} for {}. {}",
                            driverFactoryClassName,
                            resourceName,
                            e);
                }
            } else {
                LOG.warn(
                        "Could not find factory class {} for {}.",
                        driverFactoryClassName,
                        resourceName);
            }
        }

        return externalResourceDrivers;
    }

    /**
     * Instantiate {@link StaticExternalResourceInfoProvider} for all of enabled external resources.
     */
    public static ExternalResourceInfoProvider createStaticExternalResourceInfoProvider(
            Map<String, Long> externalResourceAmountMap,
            Map<String, ExternalResourceDriver> externalResourceDrivers) {
        final Map<String, Set<? extends ExternalResourceInfo>> externalResources = new HashMap<>();
        for (Map.Entry<String, ExternalResourceDriver> externalResourceDriverEntry :
                externalResourceDrivers.entrySet()) {
            final String resourceName = externalResourceDriverEntry.getKey();
            final ExternalResourceDriver externalResourceDriver =
                    externalResourceDriverEntry.getValue();
            if (externalResourceAmountMap.containsKey(resourceName)) {
                try {
                    final Set<? extends ExternalResourceInfo> externalResourceInfos;
                    externalResourceInfos =
                            externalResourceDriver.retrieveResourceInfo(
                                    externalResourceAmountMap.get(resourceName));
                    externalResources.put(resourceName, externalResourceInfos);
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to retrieve information of external resource {}.",
                            resourceName,
                            e);
                }
            } else {
                LOG.warn("Could not found legal amount configuration for {}.", resourceName);
            }
        }
        return new StaticExternalResourceInfoProvider(externalResources);
    }
}
