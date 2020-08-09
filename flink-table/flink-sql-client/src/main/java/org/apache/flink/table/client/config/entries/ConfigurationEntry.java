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

package org.apache.flink.table.client.config.entries;

import org.apache.flink.table.client.config.ConfigUtil;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Configuration for configuring {@link org.apache.flink.table.api.TableConfig}.
 * This class parses the `configuring` section in an environment file.
 *
 * <p>The configuration describes properties that would usually be defined in
 * the flink-conf.yaml file or user defined properties.
 */
public class ConfigurationEntry extends ConfigEntry {

	public static final ConfigurationEntry DEFAULT_INSTANCE =
		new ConfigurationEntry(new DescriptorProperties(true));

	private ConfigurationEntry(DescriptorProperties properties) {
		super(properties);
	}

	@Override
	protected void validate(DescriptorProperties properties) {
		// Nothing to validate as the planner will check the options
	}

	// --------------------------------------------------------------------------------------------

	public static ConfigurationEntry create(Map<String, Object> config) {
		return new ConfigurationEntry(ConfigUtil.normalizeYaml(config));
	}

	/**
	 * Merges two configuration entries. The properties of the first configuration entry might be
	 * overwritten by the second one.
	 */
	public static ConfigurationEntry merge(ConfigurationEntry configuration1, ConfigurationEntry configuration2) {
		final Map<String, String> mergedProperties = new HashMap<>(configuration1.asMap());
		mergedProperties.putAll(configuration2.asMap());

		final DescriptorProperties properties = new DescriptorProperties(true);
		properties.putProperties(mergedProperties);

		return new ConfigurationEntry(properties);
	}

	public static ConfigurationEntry enrich(
			ConfigurationEntry configuration,
			Map<String, String> remainingPrefixedProperties) {
		final Map<String, String> enrichedProperties = new HashMap<>(configuration.asMap());

		Iterator<Map.Entry<String, String>> it = remainingPrefixedProperties.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = it.next();
			enrichedProperties.put(entry.getKey().toLowerCase(), entry.getValue());
			it.remove();
		}

		final DescriptorProperties properties = new DescriptorProperties(true);
		properties.putProperties(enrichedProperties);

		return new ConfigurationEntry(properties);
	}
}
