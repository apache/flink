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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Utility class for external resource framework.
 */
public class ExternalResourceUtils {

	private static final Logger LOG = LoggerFactory.getLogger(ExternalResourceUtils.class);

	private ExternalResourceUtils() {
		throw new UnsupportedOperationException("This class should never be instantiated.");
	}

	/**
	 * Get the enabled external resource list from configuration.
	 */
	private static Set<String> getExternalResourceSet(Configuration config) {
		return new HashSet<>(config.get(ExternalResourceOptions.EXTERNAL_RESOURCE_LIST));
	}

	/**
	 * Get the external resources map.
	 */
	public static Map<String, Long> getExternalResources(Configuration config, String suffix) {
		final Set<String> resourceSet = getExternalResourceSet(config);
		LOG.info("Enabled external resources: {}", resourceSet);

		if (resourceSet.isEmpty()) {
			return Collections.emptyMap();
		}

		final Map<String, Long> externalResourceConfigs = new HashMap<>();
		for (String resourceName: resourceSet) {
			final ConfigOption<Long> amountOption =
				key(ExternalResourceOptions.getAmountConfigOptionForResource(resourceName))
					.longType()
					.noDefaultValue();
			final ConfigOption<String> configKeyOption =
				key(ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(resourceName, suffix))
					.stringType()
					.noDefaultValue();
			final String configKey = config.getString(configKeyOption);
			final Optional<Long> amountOpt = config.getOptional(amountOption);

			if (StringUtils.isNullOrWhitespaceOnly(configKey)) {
				LOG.warn("Could not find valid {} for {}. Will ignore that resource.", configKeyOption.key(), resourceName);
				continue;
			}
			if (!amountOpt.isPresent()) {
				LOG.warn("The amount of the {} should be configured. Will ignore that resource.", resourceName);
				continue;
			} else if (amountOpt.get() <= 0) {
				LOG.warn("The amount of the {} should be positive while finding {}. Will ignore that resource.", amountOpt.get(), resourceName);
				continue;
			}

			if (externalResourceConfigs.put(configKey, amountOpt.get()) != null) {
				LOG.warn("Duplicate config key {} occurred for external resources, the one named {} with amount {} will overwrite the value.", configKey, resourceName, amountOpt);
			} else {
				LOG.info("Add external resource config for {} with key {} value {}.", resourceName, configKey, amountOpt);
			}
		}

		return externalResourceConfigs;
	}
}
