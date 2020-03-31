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

package org.apache.flink.api.java.io.jdbc.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Catalogs for relational databases via JDBC.
 */
@PublicEvolving
public class JDBCCatalog extends AbstractJDBCCatalog {

	private static final Logger LOG = LoggerFactory.getLogger(JDBCCatalog.class);

	private final AbstractJDBCCatalog internal;

	public JDBCCatalog(String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
		super(catalogName, defaultDatabase, username, pwd, baseUrl);

		internal = JDBCCatalogUtils.createCatalog(catalogName, defaultDatabase, username, pwd, baseUrl);
	}

	// ------ databases -----

	@Override
	public List<String> listDatabases() throws CatalogException {
		return internal.listDatabases();
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		return internal.getDatabase(databaseName);
	}

	// ------ tables and views ------

	@Override
	public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
		return internal.listTables(databaseName);
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		return internal.getTable(tablePath);
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		try {
			return databaseExists(tablePath.getDatabaseName()) &&
				listTables(tablePath.getDatabaseName()).contains(tablePath.getObjectName());
		} catch (DatabaseNotExistException e) {
			return false;
		}
	}

	// ------ getters ------

	@VisibleForTesting
	public AbstractJDBCCatalog getInternal() {
		return internal;
	}
}
