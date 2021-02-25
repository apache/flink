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
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.TestingPluginManager;
import org.apache.flink.util.TestLogger;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link ExternalResourceUtils} class. */
public class ExternalResourceUtilsTest extends TestLogger {

    private static final String RESOURCE_NAME_1 = "foo";
    private static final String RESOURCE_NAME_2 = "bar";
    private static final List<String> RESOURCE_LIST =
            Arrays.asList(RESOURCE_NAME_1, RESOURCE_NAME_2);
    private static final long RESOURCE_AMOUNT_1 = 2L;
    private static final long RESOURCE_AMOUNT_2 = 1L;
    private static final String RESOURCE_CONFIG_KEY_1 = "flink1";
    private static final String RESOURCE_CONFIG_KEY_2 = "flink2";
    private static final String SUFFIX = "flink.config-key";

    @Test
    public void testGetExternalResourceConfigurationKeysWithConfigKeyNotSpecifiedOrEmpty() {
        final Configuration config = new Configuration();
        final String resourceConfigKey = "";

        config.set(ExternalResourceOptions.EXTERNAL_RESOURCE_LIST, RESOURCE_LIST);
        config.setString(
                ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(
                        RESOURCE_NAME_1, SUFFIX),
                resourceConfigKey);

        final Map<String, String> configMap =
                ExternalResourceUtils.getExternalResourceConfigurationKeys(config, SUFFIX);

        assertThat(configMap.entrySet(), is(empty()));
    }

    @Test
    public void testGetExternalResourceConfigurationKeysWithConflictConfigKey() {
        final Configuration config = new Configuration();

        config.set(ExternalResourceOptions.EXTERNAL_RESOURCE_LIST, RESOURCE_LIST);
        config.setString(
                ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(
                        RESOURCE_NAME_1, SUFFIX),
                RESOURCE_CONFIG_KEY_1);
        config.setString(
                ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(
                        RESOURCE_NAME_2, SUFFIX),
                RESOURCE_CONFIG_KEY_1);

        final Map<String, String> configMap =
                ExternalResourceUtils.getExternalResourceConfigurationKeys(config, SUFFIX);

        // Only one of the resource name would be kept.
        assertThat(configMap.size(), is(1));
        assertThat(configMap.values(), contains(RESOURCE_CONFIG_KEY_1));
    }

    @Test
    public void testConstructExternalResourceDriversFromConfig() {
        final Configuration config = new Configuration();
        final String driverFactoryClassName = TestingExternalResourceDriverFactory.class.getName();
        final Map<Class<?>, Iterator<?>> plugins = new HashMap<>();
        plugins.put(
                ExternalResourceDriverFactory.class,
                IteratorUtils.singletonIterator(new TestingExternalResourceDriverFactory()));
        final PluginManager testingPluginManager = new TestingPluginManager(plugins);

        config.set(
                ExternalResourceOptions.EXTERNAL_RESOURCE_LIST,
                Collections.singletonList(RESOURCE_NAME_1));
        config.setString(
                ExternalResourceOptions.getExternalResourceDriverFactoryConfigOptionForResource(
                        RESOURCE_NAME_1),
                driverFactoryClassName);

        final Map<String, ExternalResourceDriver> externalResourceDrivers =
                ExternalResourceUtils.externalResourceDriversFromConfig(
                        config, testingPluginManager);

        assertThat(externalResourceDrivers.size(), is(1));
        assertThat(
                externalResourceDrivers.get(RESOURCE_NAME_1),
                instanceOf(TestingExternalResourceDriver.class));
    }

    @Test
    public void testNotConfiguredFactoryClass() {
        final Configuration config = new Configuration();
        final Map<Class<?>, Iterator<?>> plugins = new HashMap<>();
        plugins.put(
                ExternalResourceDriverFactory.class,
                IteratorUtils.singletonIterator(new TestingExternalResourceDriverFactory()));
        final PluginManager testingPluginManager = new TestingPluginManager(plugins);

        config.set(
                ExternalResourceOptions.EXTERNAL_RESOURCE_LIST,
                Collections.singletonList(RESOURCE_NAME_1));

        final Map<String, ExternalResourceDriver> externalResourceDrivers =
                ExternalResourceUtils.externalResourceDriversFromConfig(
                        config, testingPluginManager);

        assertThat(externalResourceDrivers.entrySet(), is(empty()));
    }

    @Test
    public void testFactoryPluginDoesNotExist() {
        final Configuration config = new Configuration();
        final String driverFactoryClassName = TestingExternalResourceDriverFactory.class.getName();
        final PluginManager testingPluginManager = new TestingPluginManager(Collections.emptyMap());

        config.set(
                ExternalResourceOptions.EXTERNAL_RESOURCE_LIST,
                Collections.singletonList(RESOURCE_NAME_1));
        config.setString(
                ExternalResourceOptions.getExternalResourceDriverFactoryConfigOptionForResource(
                        RESOURCE_NAME_1),
                driverFactoryClassName);

        final Map<String, ExternalResourceDriver> externalResourceDrivers =
                ExternalResourceUtils.externalResourceDriversFromConfig(
                        config, testingPluginManager);

        assertThat(externalResourceDrivers.entrySet(), is(empty()));
    }

    @Test
    public void testFactoryFailedToCreateDriver() {
        final Configuration config = new Configuration();
        final String driverFactoryClassName =
                TestingFailedExternalResourceDriverFactory.class.getName();
        final Map<Class<?>, Iterator<?>> plugins = new HashMap<>();
        plugins.put(
                ExternalResourceDriverFactory.class,
                IteratorUtils.singletonIterator(new TestingFailedExternalResourceDriverFactory()));
        final PluginManager testingPluginManager = new TestingPluginManager(plugins);

        config.set(
                ExternalResourceOptions.EXTERNAL_RESOURCE_LIST,
                Collections.singletonList(RESOURCE_NAME_1));
        config.setString(
                ExternalResourceOptions.getExternalResourceDriverFactoryConfigOptionForResource(
                        RESOURCE_NAME_1),
                driverFactoryClassName);

        final Map<String, ExternalResourceDriver> externalResourceDrivers =
                ExternalResourceUtils.externalResourceDriversFromConfig(
                        config, testingPluginManager);

        assertThat(externalResourceDrivers.entrySet(), is(empty()));
    }

    @Test
    public void testGetExternalResourceInfoProvider() {
        final Map<String, Long> externalResourceAmountMap = new HashMap<>();
        final Map<String, ExternalResourceDriver> externalResourceDrivers = new HashMap<>();
        externalResourceAmountMap.put(RESOURCE_NAME_1, RESOURCE_AMOUNT_1);
        externalResourceDrivers.put(RESOURCE_NAME_1, new TestingExternalResourceDriver());

        final StaticExternalResourceInfoProvider externalResourceInfoProvider =
                (StaticExternalResourceInfoProvider)
                        ExternalResourceUtils.createStaticExternalResourceInfoProvider(
                                externalResourceAmountMap, externalResourceDrivers);

        assertNotNull(externalResourceInfoProvider.getExternalResources().get(RESOURCE_NAME_1));
    }

    @Test
    public void testGetExternalResourceInfoProviderWithoutAmount() {
        final Map<String, Long> externalResourceAmountMap = new HashMap<>();
        final Map<String, ExternalResourceDriver> externalResourceDrivers = new HashMap<>();
        externalResourceDrivers.put(RESOURCE_NAME_1, new TestingExternalResourceDriver());

        final StaticExternalResourceInfoProvider externalResourceInfoProvider =
                (StaticExternalResourceInfoProvider)
                        ExternalResourceUtils.createStaticExternalResourceInfoProvider(
                                externalResourceAmountMap, externalResourceDrivers);

        assertThat(externalResourceInfoProvider.getExternalResources().entrySet(), is(empty()));
    }

    @Test
    public void testGetExternalResourceAmountMap() {
        final Configuration config = new Configuration();
        config.set(
                ExternalResourceOptions.EXTERNAL_RESOURCE_LIST,
                Collections.singletonList(RESOURCE_NAME_1));
        config.setLong(
                ExternalResourceOptions.getAmountConfigOptionForResource(RESOURCE_NAME_1),
                RESOURCE_AMOUNT_1);

        final Map<String, Long> externalResourceAmountMap =
                ExternalResourceUtils.getExternalResourceAmountMap(config);

        assertThat(externalResourceAmountMap.size(), is(1));
        assertTrue(externalResourceAmountMap.containsKey(RESOURCE_NAME_1));
        assertThat(externalResourceAmountMap.get(RESOURCE_NAME_1), is(RESOURCE_AMOUNT_1));
    }

    @Test
    public void testGetExternalResourceAmountMapWithIllegalAmount() {
        final Configuration config = new Configuration();
        config.set(
                ExternalResourceOptions.EXTERNAL_RESOURCE_LIST,
                Collections.singletonList(RESOURCE_NAME_1));
        config.setLong(
                ExternalResourceOptions.getAmountConfigOptionForResource(RESOURCE_NAME_1), 0);

        final Map<String, Long> externalResourceAmountMap =
                ExternalResourceUtils.getExternalResourceAmountMap(config);

        assertThat(externalResourceAmountMap.entrySet(), is(empty()));
    }

    @Test
    public void testGetExternalResourcesCollection() {
        final Configuration config = new Configuration();
        config.set(
                ExternalResourceOptions.EXTERNAL_RESOURCE_LIST,
                Collections.singletonList(RESOURCE_NAME_1));
        config.setLong(
                ExternalResourceOptions.getAmountConfigOptionForResource(RESOURCE_NAME_1),
                RESOURCE_AMOUNT_1);

        final Collection<ExternalResource> externalResources =
                ExternalResourceUtils.getExternalResourcesCollection(config);

        assertThat(externalResources.size(), is(1));
        assertThat(
                externalResources,
                contains(new ExternalResource(RESOURCE_NAME_1, RESOURCE_AMOUNT_1)));
    }

    @Test
    public void testRecognizeEmptyResourceList() {
        final Configuration config = new Configuration();
        config.setString(
                ExternalResourceOptions.EXTERNAL_RESOURCE_LIST.key(), ExternalResourceOptions.NONE);
        config.setLong(
                ExternalResourceOptions.getAmountConfigOptionForResource(RESOURCE_NAME_1),
                RESOURCE_AMOUNT_1);

        final Collection<ExternalResource> externalResources =
                ExternalResourceUtils.getExternalResourcesCollection(config);

        assertThat(externalResources, is(empty()));
    }
}
