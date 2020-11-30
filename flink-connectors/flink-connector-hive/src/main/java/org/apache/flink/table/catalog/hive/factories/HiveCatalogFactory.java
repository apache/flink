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

package org.apache.flink.table.catalog.hive.factories;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericCachedCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator.CATALOG_HADOOP_CONF_DIR;
import static org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator.CATALOG_HIVE_CONF_DIR;
import static org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator.CATALOG_HIVE_VERSION;
import static org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator.CATALOG_TYPE_VALUE_HIVE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_ASYNC_RELOAD;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_ENABLE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_EXECUTOR_SIZE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_MAXIMUM_SIZE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_REFRESH_INTERVAL;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_TTL;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_ASYNC_RELOAD;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_ENABLE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_EXECUTOR_SIZE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_MAXIMUM_SIZE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_REFRESH_INTERVAL;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.DEFAULT_CATALOG_CACHE_TTL;

/**
 * Catalog factory for {@link HiveCatalog}.
 */
public class HiveCatalogFactory implements CatalogFactory {
	private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogFactory.class);

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_HIVE); // hive
		context.put(CATALOG_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// default database
		properties.add(CATALOG_DEFAULT_DATABASE);

		properties.add(CATALOG_HIVE_CONF_DIR);

		properties.add(CATALOG_HIVE_VERSION);

		properties.add(CATALOG_HADOOP_CONF_DIR);

		properties.add(CATALOG_CACHE_ENABLE);

		properties.add(CATALOG_CACHE_ASYNC_RELOAD);

		properties.add(CATALOG_CACHE_EXECUTOR_SIZE);

		properties.add(CATALOG_CACHE_TTL);

		properties.add(CATALOG_CACHE_REFRESH_INTERVAL);

		properties.add(CATALOG_CACHE_MAXIMUM_SIZE);

		return properties;
	}

	@Override
	public Catalog createCatalog(String name, Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final String defaultDatabase =
			descriptorProperties.getOptionalString(CATALOG_DEFAULT_DATABASE)
				.orElse(HiveCatalog.DEFAULT_DB);

		final Optional<String> hiveConfDir = descriptorProperties.getOptionalString(CATALOG_HIVE_CONF_DIR);

		final Optional<String> hadoopConfDir = descriptorProperties.getOptionalString(CATALOG_HADOOP_CONF_DIR);

		final String version = descriptorProperties.getOptionalString(CATALOG_HIVE_VERSION).orElse(HiveShimLoader.getHiveVersion());

		final boolean cacheEnable = descriptorProperties.getOptionalBoolean(CATALOG_CACHE_ENABLE)
			.orElse(DEFAULT_CATALOG_CACHE_ENABLE);

		HiveCatalog hiveCatalog = new HiveCatalog(
			name, defaultDatabase, hiveConfDir.orElse(null), hadoopConfDir.orElse(null), version);
		if (cacheEnable) {
			int executorSize = descriptorProperties.getOptionalInt(CATALOG_CACHE_EXECUTOR_SIZE)
				.orElse(DEFAULT_CATALOG_CACHE_EXECUTOR_SIZE);
			Duration ttl = descriptorProperties.getOptionalDuration(CATALOG_CACHE_TTL)
				.orElse(DEFAULT_CATALOG_CACHE_TTL);
			Duration refresh = descriptorProperties.getOptionalDuration(CATALOG_CACHE_REFRESH_INTERVAL)
				.orElse(DEFAULT_CATALOG_CACHE_REFRESH_INTERVAL);
			boolean asyncReloadEnabled = descriptorProperties.getOptionalBoolean(CATALOG_CACHE_ASYNC_RELOAD)
				.orElse(DEFAULT_CATALOG_CACHE_ASYNC_RELOAD);
			long maxSize = descriptorProperties.getOptionalLong(CATALOG_CACHE_MAXIMUM_SIZE)
				.orElse(DEFAULT_CATALOG_CACHE_MAXIMUM_SIZE);
			return new GenericCachedCatalog(
				hiveCatalog, name, defaultDatabase, asyncReloadEnabled, executorSize, ttl, refresh, maxSize);
		} else {
			return hiveCatalog;
		}
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new HiveCatalogValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
