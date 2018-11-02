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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.CatalogAlreadyExistException;
import org.apache.flink.table.api.CatalogNotExistException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory testing implementation of {@link ExternalCatalog}.
 * It could be used for testing or developing instead of used in production environment.
 */
public class TestingInMemoryCatalog extends FlinkInMemoryCatalog {

	private final Map<String, ExternalCatalog> databases = new ConcurrentHashMap<>();

	public TestingInMemoryCatalog(String catalogName) {
		super(catalogName);
	}

	@Override
	public void createSubCatalog(String name, ExternalCatalog catalog, boolean ignoreIfExists)
		throws CatalogAlreadyExistException {
		if (databases.containsKey(name)) {
			if (!ignoreIfExists) {
				throw new CatalogAlreadyExistException(name);
			}
		} else {
			databases.put(name, catalog);
		}
	}

	@Override
	public void dropSubCatalog(String name, boolean ignoreIfNotExists)
		throws CatalogNotExistException {
		if (databases.remove(name) == null && !ignoreIfNotExists) {
			throw new CatalogNotExistException(name);
		}
	}

	@Override
	public void alterSubCatalog(String name, ExternalCatalog catalog, boolean ignoreIfNotExists)
		throws CatalogNotExistException {
		if (databases.containsKey(name)) {
			databases.put(name, catalog);
		} else if (!ignoreIfNotExists) {
			throw new CatalogNotExistException(name);
		}
	}

	@Override
	public ExternalCatalog getSubCatalog(String dbName) throws CatalogNotExistException {
		ExternalCatalog result = databases.get(dbName);

		if (result == null) {
			throw new CatalogNotExistException(dbName, null);
		} else {
			return result;
		}
	}

	@Override
	public List<String> listSubCatalogs() {
		return new ArrayList<>(databases.keySet());
	}
}
