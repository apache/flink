/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog;

import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.StringUtils;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Dynamic loader for catalogs.
 */
public class CatalogLoader {

	// ------------------------------------------------------------------------
	//  Configuration shortcut names
	// ------------------------------------------------------------------------

	// The shortcut configuration name for FlinkInMemoryCatalog
	public static final String FLINK_IN_MEMORY_CATALOG_NAME = "inmemory";

	// The shortcut configuration name for HiveCatalog
	public static final String HIVE_CATALOG_NAME = "hive";

	private static final String HIVE_CATALOG_FACTORY_CLASS_NAME = "org.apache.flink.table.catalog.hive.HiveCatalogFactory";

	// ------------------------------------------------------------------------
	//  Loading the state backend from a configuration
	// ------------------------------------------------------------------------

	public static ReadableCatalog loadCatalogFromConfig(
		ClassLoader cl,
		String catalogType,
		String catalogName,
		Map<String, String> properties) throws DynamicCodeLoadingException {

		checkNotNull(cl, "class loader cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogType), "catalogType cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkNotNull(properties, "properties cannot be null or empty");

		switch (catalogType.toLowerCase()) {
			case FLINK_IN_MEMORY_CATALOG_NAME:
				return new FlinkInMemoryCatalogFactory().createCatalog(catalogName, properties);
			case HIVE_CATALOG_NAME:
				return loadCatalog(
					HIVE_CATALOG_FACTORY_CLASS_NAME, cl, catalogType, catalogName, properties);
			default:
				// To use self-defined catalog, user have to put the catalog's full class name as catalog type in config file
				return loadCatalog(catalogType, cl, catalogType, catalogName, properties);
		}
	}

	private static ReadableCatalog loadCatalog(
		String factoryClassName,
		ClassLoader cl,
		String catalogType,
		String catalogName,
		Map<String, String> properties) throws DynamicCodeLoadingException {

		CatalogFactory<?> factory;

		try {
			Class<? extends CatalogFactory> clazz = Class.forName(factoryClassName, false, cl)
				.asSubclass(CatalogFactory.class);
			factory = clazz.newInstance();
		} catch (ClassNotFoundException e) {
			throw new DynamicCodeLoadingException(
				String.format("Cannot find configured catalog factory class: %s", catalogType), e);
		} catch (ClassCastException | InstantiationException | IllegalAccessException e) {
			throw new DynamicCodeLoadingException(
				String.format(
					"The class configured for catalog does not have a valid catalog factory (%s)", catalogType), e);
		}

		return factory.createCatalog(catalogName, properties);
	}
}
