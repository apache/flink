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

package org.apache.flink.table.client.catalog;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.catalog.FlinkInMemoryCatalogFactory;
import org.apache.flink.table.catalog.ReadableCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalogFactory;
import org.apache.flink.table.client.config.entries.CatalogEntry;

import java.util.HashMap;
import java.util.Map;

/**
 * The factory used to create ReadableCatalog from the given configuration CatalogEntry.
 */
public class ClientCatalogFactory {
	private static final String PREFIXES_TO_CLEAN = CatalogEntry.CATALOG_CONNECTOR_PREFIX + ".";

	public static ReadableCatalog createCatalog(CatalogEntry catalog) {
		// TODO: convert to service discovery style

		CatalogType type = CatalogType.valueOf(catalog.getProperties().getString(CatalogEntry.CATALOG_TYPE));
		Map<String, String> cleaned = cleanProperties(catalog.getProperties().asMap());

		switch (type) {
			case hive:
				return new HiveCatalogFactory().createCatalog(catalog.getName(), cleaned);
			case flink_in_memory:
				return new FlinkInMemoryCatalogFactory().createCatalog(catalog.getName(), cleaned);
			default:
				throw new IllegalArgumentException("Doesn't support catalog type " + type + " yet.");
		}
	}

	@VisibleForTesting
	protected static Map<String, String> cleanProperties(Map<String, String> prop) {
		Map<String, String> cleaned = new HashMap<>();

		for (Map.Entry<String, String> e : prop.entrySet()) {
			String key = e.getKey();

			if (key.startsWith(PREFIXES_TO_CLEAN)) {
				key = key.substring(key.indexOf(PREFIXES_TO_CLEAN) + PREFIXES_TO_CLEAN.length());
			}

			cleaned.put(key, e.getValue());
		}

		return cleaned;
	}
}
