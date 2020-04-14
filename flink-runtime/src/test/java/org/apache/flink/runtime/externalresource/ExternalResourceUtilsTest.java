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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link ExternalResourceUtils} class.
 */
public class ExternalResourceUtilsTest extends TestLogger {

	private static final String RESOURCE_NAME_1 = "foo";
	private static final String RESOURCE_NAME_2 = "bar";
	private static final List<String> RESOURCE_LIST = Arrays.asList(RESOURCE_NAME_1, RESOURCE_NAME_2);
	private static final long RESOURCE_AMOUNT_1 = 2L;
	private static final long RESOURCE_AMOUNT_2 = 1L;
	private static final String RESOURCE_CONFIG_KEY_1 = "flink1";
	private static final String RESOURCE_CONFIG_KEY_2 = "flink2";
	private static final String SUFFIX = "flink.config-key";

	@Test
	public void testGetExternalResourcesWithConfigKeyNotSpecifiedOrEmpty() {
		final Configuration config = new Configuration();
		final String resourceConfigKey = "";

		config.set(ExternalResourceOptions.EXTERNAL_RESOURCE_LIST, RESOURCE_LIST);
		config.setLong(ExternalResourceOptions.getAmountConfigOptionForResource(RESOURCE_NAME_1), RESOURCE_AMOUNT_1);
		config.setLong(ExternalResourceOptions.getAmountConfigOptionForResource(RESOURCE_NAME_2), RESOURCE_AMOUNT_2);
		config.setString(ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(RESOURCE_NAME_1, SUFFIX), resourceConfigKey);

		final Map<String, Long> configMap = ExternalResourceUtils.getExternalResources(config, SUFFIX);

		assertThat(configMap.entrySet(), is(empty()));
	}

	@Test
	public void testGetExternalResourcesWithIllegalAmount() {
		final Configuration config = new Configuration();
		final long resourceAmount = 0L;

		config.set(ExternalResourceOptions.EXTERNAL_RESOURCE_LIST, RESOURCE_LIST);
		config.setLong(ExternalResourceOptions.getAmountConfigOptionForResource(RESOURCE_NAME_1), resourceAmount);
		config.setString(ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(RESOURCE_NAME_1, SUFFIX), RESOURCE_CONFIG_KEY_1);

		final Map<String, Long> configMap = ExternalResourceUtils.getExternalResources(config, SUFFIX);

		assertThat(configMap.entrySet(), is(empty()));
	}

	@Test
	public void testGetExternalResourcesWithoutConfigAmount() {
		final Configuration config = new Configuration();

		config.set(ExternalResourceOptions.EXTERNAL_RESOURCE_LIST, RESOURCE_LIST);
		config.setString(ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(RESOURCE_NAME_1, SUFFIX), RESOURCE_CONFIG_KEY_1);

		final Map<String, Long> configMap = ExternalResourceUtils.getExternalResources(config, SUFFIX);

		assertThat(configMap.entrySet(), is(empty()));
	}

	@Test
	public void testGetExternalResourcesWithConflictConfigKey() {
		final Configuration config = new Configuration();

		config.set(ExternalResourceOptions.EXTERNAL_RESOURCE_LIST, RESOURCE_LIST);
		config.setLong(ExternalResourceOptions.getAmountConfigOptionForResource(RESOURCE_NAME_1), RESOURCE_AMOUNT_1);
		config.setLong(ExternalResourceOptions.getAmountConfigOptionForResource(RESOURCE_NAME_2), RESOURCE_AMOUNT_2);
		config.setString(ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(RESOURCE_NAME_1, SUFFIX), RESOURCE_CONFIG_KEY_1);
		config.setString(ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(RESOURCE_NAME_2, SUFFIX), RESOURCE_CONFIG_KEY_1);

		final Map<String, Long> configMap = ExternalResourceUtils.getExternalResources(config, SUFFIX);

		// Only one of the config key would be kept.
		assertThat(configMap.size(), is(1));
		assertTrue(configMap.containsKey(RESOURCE_CONFIG_KEY_1));
	}

	@Test
	public void testGetExternalResourcesWithMultipleExternalResource() {
		final Configuration config = new Configuration();

		config.set(ExternalResourceOptions.EXTERNAL_RESOURCE_LIST, RESOURCE_LIST);
		config.setLong(ExternalResourceOptions.getAmountConfigOptionForResource(RESOURCE_NAME_1), RESOURCE_AMOUNT_1);
		config.setLong(ExternalResourceOptions.getAmountConfigOptionForResource(RESOURCE_NAME_2), RESOURCE_AMOUNT_2);
		config.setString(ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(RESOURCE_NAME_1, SUFFIX), RESOURCE_CONFIG_KEY_1);
		config.setString(ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(RESOURCE_NAME_2, SUFFIX), RESOURCE_CONFIG_KEY_2);

		final Map<String, Long> configMap = ExternalResourceUtils.getExternalResources(config, SUFFIX);

		assertThat(configMap.size(), is(2));
		assertTrue(configMap.containsKey(RESOURCE_CONFIG_KEY_1));
		assertTrue(configMap.containsKey(RESOURCE_CONFIG_KEY_2));
		assertThat(configMap.get(RESOURCE_CONFIG_KEY_1), is(RESOURCE_AMOUNT_1));
		assertThat(configMap.get(RESOURCE_CONFIG_KEY_2), is(RESOURCE_AMOUNT_2));
	}
}
