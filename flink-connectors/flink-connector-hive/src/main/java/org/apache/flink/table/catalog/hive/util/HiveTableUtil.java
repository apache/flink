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
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.types.DataType;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.table.catalog.hive.HiveCatalogConfig.DEFAULT_LIST_COLUMN_TYPES_SEPARATOR;

/**
 * Utils to for Hive-backed table.
 */
public class HiveTableUtil {

	private static final byte HIVE_CONSTRAINT_ENABLE = 1 << 2;
	private static final byte HIVE_CONSTRAINT_VALIDATE = 1 << 1;
	private static final byte HIVE_CONSTRAINT_RELY = 1;

	private HiveTableUtil() {
	}

	/**
	 * Create a Flink's TableSchema from Hive table's columns and partition keys.
	 */
	public static TableSchema createTableSchema(List<FieldSchema> cols, List<FieldSchema> partitionKeys,
			Set<String> notNullColumns, UniqueConstraint primaryKey) {
		List<FieldSchema> allCols = new ArrayList<>(cols);
		allCols.addAll(partitionKeys);

		String[] colNames = new String[allCols.size()];
		DataType[] colTypes = new DataType[allCols.size()];

		for (int i = 0; i < allCols.size(); i++) {
			FieldSchema fs = allCols.get(i);

			colNames[i] = fs.getName();
			colTypes[i] = HiveTypeUtil.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(fs.getType()));
			if (notNullColumns.contains(colNames[i])) {
				colTypes[i] = colTypes[i].notNull();
			}
		}

		TableSchema.Builder builder = TableSchema.builder().fields(colNames, colTypes);
		if (primaryKey != null) {
			builder.primaryKey(primaryKey.getName(), primaryKey.getColumns().toArray(new String[0]));
		}
		return builder.build();
	}

	/**
	 * Create Hive columns from Flink TableSchema.
	 */
	public static List<FieldSchema> createHiveColumns(TableSchema schema) {
		String[] fieldNames = schema.getFieldNames();
		DataType[] fieldTypes = schema.getFieldDataTypes();

		List<FieldSchema> columns = new ArrayList<>(fieldNames.length);

		for (int i = 0; i < fieldNames.length; i++) {
			columns.add(
				new FieldSchema(fieldNames[i], HiveTypeUtil.toHiveTypeName(fieldTypes[i]), null));
		}

		return columns;
	}

	// --------------------------------------------------------------------------------------------
	//  Helper methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Create properties info to initialize a SerDe.
	 * @param storageDescriptor
	 * @return
	 */
	public static Properties createPropertiesFromStorageDescriptor(StorageDescriptor storageDescriptor) {
		SerDeInfo serDeInfo = storageDescriptor.getSerdeInfo();
		Map<String, String> parameters = serDeInfo.getParameters();
		Properties properties = new Properties();
		properties.setProperty(
				serdeConstants.SERIALIZATION_FORMAT,
				parameters.get(serdeConstants.SERIALIZATION_FORMAT));
		List<String> colTypes = new ArrayList<>();
		List<String> colNames = new ArrayList<>();
		List<FieldSchema> cols = storageDescriptor.getCols();
		for (FieldSchema col: cols){
			colTypes.add(col.getType());
			colNames.add(col.getName());
		}
		properties.setProperty(serdeConstants.LIST_COLUMNS, StringUtils.join(colNames, String.valueOf(SerDeUtils.COMMA)));
		// Note: serdeConstants.COLUMN_NAME_DELIMITER is not defined in previous Hive. We use a literal to save on shim
		properties.setProperty("column.name.delimite", String.valueOf(SerDeUtils.COMMA));
		properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, StringUtils.join(colTypes, DEFAULT_LIST_COLUMN_TYPES_SEPARATOR));
		properties.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
		properties.putAll(parameters);
		return properties;
	}

	/**
	 * Creates a Hive partition instance.
	 */
	public static Partition createHivePartition(String dbName, String tableName, List<String> values,
			StorageDescriptor sd, Map<String, String> parameters) {
		Partition partition = new Partition();
		partition.setDbName(dbName);
		partition.setTableName(tableName);
		partition.setValues(values);
		partition.setParameters(parameters);
		partition.setSd(sd);
		int currentTime = (int) (System.currentTimeMillis() / 1000);
		partition.setCreateTime(currentTime);
		partition.setLastAccessTime(currentTime);
		return partition;
	}

	// returns a constraint trait that requires ENABLE
	public static byte enableConstraint(byte trait) {
		return (byte) (trait | HIVE_CONSTRAINT_ENABLE);
	}

	// returns a constraint trait that requires VALIDATE
	public static byte validateConstraint(byte trait) {
		return (byte) (trait | HIVE_CONSTRAINT_VALIDATE);
	}

	// returns a constraint trait that requires RELY
	public static byte relyConstraint(byte trait) {
		return (byte) (trait | HIVE_CONSTRAINT_RELY);
	}

	// returns whether a trait requires ENABLE constraint
	public static boolean requireEnableConstraint(byte trait) {
		return (trait & HIVE_CONSTRAINT_ENABLE) != 0;
	}

	// returns whether a trait requires VALIDATE constraint
	public static boolean requireValidateConstraint(byte trait) {
		return (trait & HIVE_CONSTRAINT_VALIDATE) != 0;
	}

	// returns whether a trait requires RELY constraint
	public static boolean requireRelyConstraint(byte trait) {
		return (trait & HIVE_CONSTRAINT_RELY) != 0;
	}

}
