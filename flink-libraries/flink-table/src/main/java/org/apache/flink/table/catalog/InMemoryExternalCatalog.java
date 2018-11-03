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
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is an in-memory implementation of {@link ExternalCatalog}.
 *
 * <p>It could be used for testing or developing instead of used in production environment.
 */
public class InMemoryExternalCatalog implements CrudExternalCatalog {

	// The name of the catalog
	private final String catalogName;

	private final Map<String, ExternalCatalogTable> tables = new HashMap<>();
	private final Map<String, ExternalCatalog> databases = new HashMap<>();

	public InMemoryExternalCatalog(String name) {
		this.catalogName = name;
	}

	@Override
	public void createTable(String tableName, ExternalCatalogTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException {
		if (tables.containsKey(tableName)) {
			if (!ignoreIfExists) {
				throw new TableAlreadyExistException(catalogName, tableName);
			}
		} else {
			tables.put(tableName, table);
		}
	}

	@Override
	public void dropTable(String tableName, boolean ignoreIfNotExists)
		throws TableNotExistException {
		if (tables.remove(tableName) == null && !ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tableName);
		}
	}

	@Override
	public void alterTable(String tableName, ExternalCatalogTable table, boolean ignoreIfNotExists)
		throws TableNotExistException {
		if (tables.containsKey(tableName)) {
			tables.put(tableName, table);
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tableName);
		}
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
	public ExternalCatalogTable getTable(String tableName) throws TableNotExistException {
		ExternalCatalogTable result = tables.get(tableName);
		if (result == null) {
			throw new TableNotExistException(catalogName, tableName, null);
		} else {
			return result;
		}
	}

	@Override
	public List<String> listTables() {
		return new ArrayList<>(tables.keySet());
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
