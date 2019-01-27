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

import org.apache.flink.table.catalog.hive.config.HiveCatalogConfig;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.config.ConfigUtil;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Descriptor for Catalog.
 */
public class CatalogEntry extends ConfigEntry {
	private static final String CATALOG_NAME = "name";
	public static final String CATALOG_CONNECTOR_PREFIX = "catalog.connector";
	public static final String CATALOG_TYPE = "catalog.type";
	public static final String CATALOG_IS_DEFAULT = "catalog.is-default";
	public static final String CATALOG_DEFAULT_DB = "catalog.default-database";
	// Hive-specific
	public static final String CATALOG_CONNECTOR_HIVE_METASTORE_URIS =
		CATALOG_CONNECTOR_PREFIX + "." + HiveCatalogConfig.HIVE_METASTORE_URIS;
	public static final String CATALOG_CONNECTOR_HIVE_METASTORE_USERNAME =
		CATALOG_CONNECTOR_PREFIX + "." + HiveCatalogConfig.HIVE_METASTORE_USERNAME;

	private String name;
	private DescriptorProperties properties;

	public CatalogEntry(String name, DescriptorProperties properties) {
		super(properties);
		this.name = name;
		this.properties = properties;
	}

	public String getName() {
		return name;
	}

	public DescriptorProperties getProperties() {
		return properties;
	}

	public boolean isDefaultCatalog() {
		if (!properties.containsKey(CATALOG_IS_DEFAULT)) {
			return false;
		} else {
			String s = properties.getString(CATALOG_IS_DEFAULT);
			return Boolean.valueOf(s);
		}
	}

	public Optional<String> getType() {
		if (!properties.containsKey(CATALOG_TYPE)) {
			return Optional.empty();
		} else {
			String s = properties.getString(CATALOG_TYPE);
			return Optional.of(s);
		}
	}

	public Optional<String> getDefaultDatabase() {
		if (!properties.containsKey(CATALOG_DEFAULT_DB)) {
			return Optional.empty();
		} else {
			String s = properties.getString(CATALOG_DEFAULT_DB);
			return Optional.ofNullable(s);
		}
	}

	// --------------------------------------------------------------------------------------------

	public static CatalogEntry create(Map<String, Object> config) {
		final Object name = config.get(CATALOG_NAME);

		if (name == null || !(name instanceof String) || StringUtils.isNullOrWhitespaceOnly((String) name)) {
			throw new SqlClientException("Invalid function name '" + name + "'.");
		}

		final Map<String, Object> properties = new HashMap<>(config);
		properties.remove(CATALOG_NAME);
		return new CatalogEntry((String) name, ConfigUtil.normalizeYaml(properties));
	}

	@Override
	protected void validate(DescriptorProperties properties) {
		properties.validateString(CATALOG_TYPE, false, 1);
	}
}
