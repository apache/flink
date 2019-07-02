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

import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.TableNotExistException;

import java.util.List;

/**
 * An {@link ExternalCatalog} is the connector between an external database catalog and Flink's
 * Table API.
 *
 * <p>It provides information about catalogs, databases and tables such as names, schema,
 * statistics, and access information.
 *
 * @deprecated use {@link Catalog} instead.
 */
@Deprecated
public interface ExternalCatalog {

	/**
	 * Get a table from this catalog.
	 *
	 * @param tableName The name of the table.
	 * @return The requested table.
	 * @throws TableNotExistException thrown if the table does not exist in the catalog.
	 */
	ExternalCatalogTable getTable(String tableName) throws TableNotExistException;

	/**
	 * Gets the names of all tables registered in this catalog.
	 *
	 * @return A list of the names of all registered tables.
	 */
	List<String> listTables();

	/**
	 * Gets a sub catalog from this catalog.
	 *
	 * @return The requested sub catalog.
	 */
	ExternalCatalog getSubCatalog(String dbName) throws CatalogNotExistException;

	/**
	 * Gets the names of all sub catalogs registered in this catalog.
	 *
	 * @return The list of the names of all registered sub catalogs.
	 */
	List<String> listSubCatalogs();

}
