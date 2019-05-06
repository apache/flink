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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.GenericCatalogTable;
import org.apache.flink.table.catalog.GenericCatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalogBaseUtil;
import org.apache.flink.table.catalog.hive.HiveTableConfig;
import org.apache.flink.table.catalog.hive.HiveTypeUtil;
import org.apache.flink.table.plan.stats.TableStats;

import org.apache.hadoop.hive.metastore.TableType;
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
 * Utils to convert meta objects between Flink and Hive for GenericHiveMetastoreCatalog.
 */
public class GenericHiveMetastoreCatalogUtil {

	// Prefix used to distinguish properties created by Hive and Flink,
	// as Hive metastore has its own properties created upon table creation and migration between different versions of metastore.
	private static final String FLINK_PROPERTY_PREFIX = "flink.";

	// Flink tables should be stored as 'external' tables in Hive metastore
	private static final Map<String, String> EXTERNAL_TABLE_PROPERTY = new HashMap<String, String>() {{
		put("EXTERNAL", "TRUE");
	}};

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

	/**
	 * Creates a Hive table from a CatalogBaseTable.
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
		hiveTable.setParameters(buildFlinkProperties(properties));
		hiveTable.getParameters().putAll(EXTERNAL_TABLE_PROPERTY);

		// Hive table's StorageDescriptor
		StorageDescriptor sd = new StorageDescriptor();
		sd.setSerdeInfo(new SerDeInfo(null, null, new HashMap<>()));

		List<FieldSchema> allColumns = createHiveColumns(table.getSchema());

		// Table columns and partition keys
		if (table instanceof CatalogTable) {
			CatalogTable catalogTable = (CatalogTable) table;

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

			hiveTable.setTableType(TableType.EXTERNAL_TABLE.name());
		} else if (table instanceof CatalogView) {
			CatalogView view = (CatalogView) table;

			// TODO: [FLINK-12398] Support partitioned view in catalog API
			sd.setCols(allColumns);
			hiveTable.setPartitionKeys(new ArrayList<>());

			hiveTable.setViewOriginalText(view.getOriginalQuery());
			hiveTable.setViewExpandedText(view.getExpandedQuery());
			hiveTable.setTableType(TableType.VIRTUAL_VIEW.name());
		} else {
			throw new IllegalArgumentException(
				"GenericHiveMetastoreCatalog only supports CatalogTable and CatalogView");
		}

		hiveTable.setSd(sd);

		return hiveTable;
	}

	/**
	 * Creates a CatalogBaseTable from a Hive table.
	 *
	 * @param hiveTable the Hive table
	 * @return a CatalogBaseTable
	 */
	public static CatalogBaseTable createCatalogTable(Table hiveTable) {
		// Table schema
		TableSchema tableSchema = HiveCatalogBaseUtil.createTableSchema(
				hiveTable.getSd().getCols(), hiveTable.getPartitionKeys());

		// Table properties
		Map<String, String> properties = retrieveFlinkProperties(hiveTable.getParameters());

		// Table comment
		String comment = properties.remove(HiveTableConfig.TABLE_COMMENT);

		// Partition keys
		List<String> partitionKeys = new ArrayList<>();

		if (!hiveTable.getPartitionKeys().isEmpty()) {
			partitionKeys = hiveTable.getPartitionKeys().stream()
								.map(fs -> fs.getName())
								.collect(Collectors.toList());
		}

		if (TableType.valueOf(hiveTable.getTableType()) == TableType.VIRTUAL_VIEW) {
			return new GenericCatalogView(
				hiveTable.getViewOriginalText(),
				hiveTable.getViewExpandedText(),
				tableSchema,
				properties,
				comment
			);
		} else {
			return new GenericCatalogTable(
				tableSchema, new TableStats(0), partitionKeys, properties, comment);
		}
	}

	/**
	 * Create Hive columns from Flink TableSchema.
	 */
	private static List<FieldSchema> createHiveColumns(TableSchema schema) {
		String[] fieldNames = schema.getFieldNames();
		TypeInformation[] fieldTypes = schema.getFieldTypes();

		List<FieldSchema> columns = new ArrayList<>(fieldNames.length);

		for (int i = 0; i < fieldNames.length; i++) {
			columns.add(
				new FieldSchema(fieldNames[i], HiveTypeUtil.toHiveType(fieldTypes[i]), null));
		}

		return columns;
	}

	/**
	 * Filter out Hive-created properties, and return Flink-created properties.
	 */
	private static Map<String, String> retrieveFlinkProperties(Map<String, String> hiveTableParams) {
		return hiveTableParams.entrySet().stream()
			.filter(e -> e.getKey().startsWith(FLINK_PROPERTY_PREFIX))
			.collect(Collectors.toMap(e -> e.getKey().replace(FLINK_PROPERTY_PREFIX, ""), e -> e.getValue()));
	}

	/**
	 * Add a prefix to Flink-created properties to distinguish them from Hive-created properties.
	 */
	public static Map<String, String> buildFlinkProperties(Map<String, String> properties) {
		return properties.entrySet().stream()
			.filter(e -> e.getKey() != null && e.getValue() != null)
			.collect(Collectors.toMap(e -> FLINK_PROPERTY_PREFIX + e.getKey(), e -> e.getValue()));
	}
}
