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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalogDatabase;
import org.apache.flink.table.catalog.hive.HiveCatalogTable;
import org.apache.flink.table.catalog.hive.HiveTableConfig;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Utils to convert meta objects between Flink and Hive for HiveCatalog.
 */
public class HiveCatalogUtil extends HiveCatalogBaseUtil {

	private HiveCatalogUtil() {
	}

	// ------ Utils ------

	/**
	 * Creates a Hive database from CatalogDatabase.
	 */
	public static Database createHiveDatabase(String dbName, CatalogDatabase db) {
		HiveCatalogDatabase hiveCatalogDatabase = (HiveCatalogDatabase) db;

		return new Database(
			dbName,
			db.getComment(),
			hiveCatalogDatabase.getLocation(),
			hiveCatalogDatabase.getProperties());

	}

	/**
	 * Creates a Hive table from a CatalogBaseTable.
	 * TODO: [FLINK-12234] Support view related operations in HiveCatalog
	 *
	 * @param tablePath path of the table
	 * @param table the CatalogBaseTable instance
	 * @return a Hive table
	 */
	public static Table createHiveTable(ObjectPath tablePath, CatalogBaseTable table) {
		Map<String, String> properties = new HashMap<>(table.getProperties());

		// Table comment
		properties.put(HiveTableConfig.TABLE_COMMENT, table.getComment());

		Table hiveTable = new Table();
		hiveTable.setDbName(tablePath.getDatabaseName());
		hiveTable.setTableName(tablePath.getObjectName());
		hiveTable.setCreateTime((int) (System.currentTimeMillis() / 1000));

		// Table properties
		hiveTable.setParameters(properties);

		// Hive table's StorageDescriptor
		// TODO: This is very basic Hive table.
		//  [FLINK-11479] Add input/output format and SerDeLib information for Hive tables in HiveCatalogUtil#createHiveTable
		StorageDescriptor sd = new StorageDescriptor();
		sd.setSerdeInfo(new SerDeInfo(null, null, new HashMap<>()));

		List<FieldSchema> allColumns = createHiveColumns(table.getSchema());

		// Table columns and partition keys
		if (table instanceof CatalogTable) {
			HiveCatalogTable catalogTable = (HiveCatalogTable) table;

			if (catalogTable.isPartitioned()) {
				int partitionKeySize = catalogTable.getPartitionKeys().size();
				List<FieldSchema> regularColumns = allColumns.subList(0, allColumns.size() - partitionKeySize);
				List<FieldSchema> partitionColumns = allColumns.subList(allColumns.size() - partitionKeySize, allColumns.size());

				sd.setCols(regularColumns);
				hiveTable.setPartitionKeys(partitionColumns);
			} else {
				sd.setCols(allColumns);
				hiveTable.setPartitionKeys(new ArrayList<>());
			}
		} else {
			throw new UnsupportedOperationException("HiveCatalog doesn't support view yet");
		}

		return hiveTable;
	}

	/**
	 * Creates a CatalogBaseTable from a Hive table.
	 * TODO: [FLINK-12234] Support view related operations in HiveCatalog
	 *
	 * @param hiveTable the Hive table
	 * @return a CatalogBaseTable
	 */
	public static CatalogBaseTable createCatalogTable(Table hiveTable) {
		// Table schema
		TableSchema tableSchema = createTableSchema(
			hiveTable.getSd().getCols(), hiveTable.getPartitionKeys());

		// Table properties
		Map<String, String> properties = hiveTable.getParameters();

		// Table comment
		String comment = properties.remove(HiveTableConfig.TABLE_COMMENT);

		// Partition keys
		List<String> partitionKeys = new ArrayList<>();

		if (hiveTable.getPartitionKeys() != null && hiveTable.getPartitionKeys().isEmpty()) {
			partitionKeys = hiveTable.getPartitionKeys().stream()
				.map(fs -> fs.getName())
				.collect(Collectors.toList());
		}

		return new HiveCatalogTable(tableSchema, partitionKeys, properties, comment);
	}
}
