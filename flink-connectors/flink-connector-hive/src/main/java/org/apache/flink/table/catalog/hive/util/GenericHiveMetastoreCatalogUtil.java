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

package org.apache.flink.table.catalog.hive.util;

import org.apache.flink.table.catalog.CatalogDatabase;

import org.apache.hadoop.hive.metastore.api.Database;

/**
 * Utils to convert meta objects between Flink and Hive for GenericHiveMetastoreCatalog.
 */
public class GenericHiveMetastoreCatalogUtil {

	private GenericHiveMetastoreCatalogUtil() {
	}

	// ------ Utils ------

	/**
	 * Creates a Hive database from a CatalogDatabase.
	 *
	 * @param databaseName name of the database
	 * @param catalogDatabase the CatalogDatabase instance
	 * @return a Hive database
	 */
	public static Database createHiveDatabase(String databaseName, CatalogDatabase catalogDatabase) {
		return new Database(
			databaseName,
			catalogDatabase.getDescription().isPresent() ? catalogDatabase.getDescription().get() : null,
			null,
			catalogDatabase.getProperties());
	}
}
