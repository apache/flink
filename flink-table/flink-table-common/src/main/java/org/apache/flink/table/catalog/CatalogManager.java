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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.CatalogAlreadyExistsException;
import org.apache.flink.table.api.CatalogNotExistException;

import java.util.Set;

/**
 * CatalogManager manages all the registered ReadableCatalog instances with unique names.
 * It has a concept of current catalog, which will be used when it is not given when referencing meta-objects.
 */
@PublicEvolving
public interface CatalogManager {

	/**
	 * Register a catalog with a unique name.
	 *
	 * @param catalogName catalog name to register
	 * @param catalog catalog to register
	 * @throws CatalogAlreadyExistsException thrown if the name is already take
	 */
	void registerCatalog(String catalogName, ReadableCatalog catalog) throws CatalogAlreadyExistsException;

	/**
	 * Get a catalog by name.
	 *
	 * @param catalogName catalog name
	 * @return the requested catalog
	 * @throws CatalogNotExistException thrown if the catalog doesn't exist
	 */
	ReadableCatalog getCatalog(String catalogName) throws CatalogNotExistException;

	/**
	 * Get names of all registered catalog.
	 *
	 * @return a set of names of registered catalogs
	 */
	Set<String> getCatalogNames();

	/**
	 * Get the current catalog.
	 *
	 * @return the current catalog
	 */
	ReadableCatalog getCurrentCatalog();

	/**
	 * Set the current catalog name.
	 *
	 * @param catalogName catalog name to set as current catalog
	 * @throws CatalogNotExistException thrown if the catalog doesn't exist
	 */
	void setCurrentCatalog(String catalogName) throws CatalogNotExistException;
}
