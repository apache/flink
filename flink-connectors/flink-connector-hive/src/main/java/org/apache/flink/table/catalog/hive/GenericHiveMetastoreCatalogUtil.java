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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.catalog.CatalogDatabase;

import org.apache.hadoop.hive.metastore.api.Database;

import java.util.Map;


/**
 * Utils to convert meta objects between Flink and Hive for GenericHiveMetastoreCatalog.
 */
public class GenericHiveMetastoreCatalogUtil {

	private GenericHiveMetastoreCatalogUtil() {
	}

	// ------ Utils ------

	/**
	 * Creates a Hive database from CatalogDatabase.
	 */
	public static Database createHiveDatabase(String dbName, CatalogDatabase db) {
		Map<String, String> props = db.getProperties();
		return new Database(
			dbName,
			db.getDescription().isPresent() ? db.getDescription().get() : null,
			null,
			props);
	}
}
