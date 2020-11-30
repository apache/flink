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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;

import java.time.Duration;

/**
 * Validator for {@link CatalogDescriptor}.
 */
@Internal
public abstract class CatalogDescriptorValidator implements DescriptorValidator {

	/**
	 * Default configuration for cached catalog.
	 */
	public static final boolean DEFAULT_CATALOG_CACHE_ENABLE = false;

	public static final boolean DEFAULT_CATALOG_CACHE_ASYNC_RELOAD = false;

	public static final int DEFAULT_CATALOG_CACHE_EXECUTOR_SIZE = 2;

	public static final Duration DEFAULT_CATALOG_CACHE_TTL = Duration.ofSeconds(60);

	public static final Duration DEFAULT_CATALOG_CACHE_REFRESH_INTERVAL = Duration.ofSeconds(40);

	public static final long DEFAULT_CATALOG_CACHE_MAXIMUM_SIZE = 10000L;

	/**
	 * Key for describing the type of the catalog. Usually used for factory discovery.ca
	 */
	public static final String CATALOG_TYPE = "type";

	/**
	 * Key for describing the property version. This property can be used for backwards
	 * compatibility in case the property format changes.
	 */
	public static final String CATALOG_PROPERTY_VERSION = "property-version";

	/**
	 * Key for describing the default database of the catalog.
	 */
	public static final String CATALOG_DEFAULT_DATABASE = "default-database";

	/**
	 * Key for describing whether to enable catalog cache.
	 */
	public static final String CATALOG_CACHE_ENABLE = "cache-enable";

	/**
	 * Key for describing whether to enable async reload catalog cache,
	 * corresponding to the underlying meta client is thread-safe or not.
	 */
	public static final String CATALOG_CACHE_ASYNC_RELOAD = "cache-async-reload";

	/**
	 * Key for thread pool size to async reload catalog cache.
	 */
	public static final String CATALOG_CACHE_EXECUTOR_SIZE = "cache-executor-size";

	/**
	 * Key for expire time after the catalog entry write into cache.
	 */
	public static final String CATALOG_CACHE_TTL = "cache-ttl";

	/**
	 * Key for refreshing entry interval of catalog cache.
	 */
	public static final String CATALOG_CACHE_REFRESH_INTERVAL = "cache-refresh-interval";

	/**
	 * Key for maximum entry size of catalog cache.
	 */
	public static final String CATALOG_CACHE_MAXIMUM_SIZE = "cache-maximum-size";

	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateString(CATALOG_TYPE, false, 1);
		properties.validateInt(CATALOG_PROPERTY_VERSION, true, 0);
		properties.validateString(CATALOG_DEFAULT_DATABASE, true, 1);
		properties.validateBoolean(CATALOG_CACHE_ENABLE, true);
		properties.validateBoolean(CATALOG_CACHE_ASYNC_RELOAD, true);
		properties.validateInt(CATALOG_CACHE_EXECUTOR_SIZE, true);
		properties.validateDuration(CATALOG_CACHE_TTL, true, 1000, 10000);
		properties.validateDuration(CATALOG_CACHE_REFRESH_INTERVAL, true, 1000, 5000);
		properties.validateLong(CATALOG_CACHE_MAXIMUM_SIZE, true);
	}
}
