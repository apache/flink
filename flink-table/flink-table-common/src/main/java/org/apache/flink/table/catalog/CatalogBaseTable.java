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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.Map;
import java.util.Optional;

/**
 * CatalogBaseTable is the common parent of table and view. It has a map of
 * key-value pairs defining the properties of the table.
 */
public interface CatalogBaseTable {

	/**
	 * @deprecated Use {@link #getOptions()}.
	 */
	@Deprecated
	Map<String, String> getProperties();

	/**
	 * Returns a map of string-based options.
	 *
	 * <p>In case of {@link CatalogTable}, these options may determine the kind of connector and its
	 * configuration for accessing the data in the external system. See {@link DynamicTableFactory}
	 * for more information.
	 */
	default Map<String, String> getOptions() {
		return getProperties();
	}

	/**
	 * Get the schema of the table.
	 *
	 * @return schema of the table/view.
	 */
	TableSchema getSchema();

	/**
	 * Get comment of the table or view.
	 *
	 * @return comment of the table/view.
	 */
	String getComment();

	/**
	 * Get a deep copy of the CatalogBaseTable instance.
	 *
	 * @return a copy of the CatalogBaseTable instance
	 */
	CatalogBaseTable copy();

	/**
	 * Get a brief description of the table or view.
	 *
	 * @return an optional short description of the table/view
	 */
	Optional<String> getDescription();

	/**
	 * Get a detailed description of the table or view.
	 *
	 * @return an optional long description of the table/view
	 */
	Optional<String> getDetailedDescription();
}
