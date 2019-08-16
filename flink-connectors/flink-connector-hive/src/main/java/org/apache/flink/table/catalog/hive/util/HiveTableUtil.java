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
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.catalog.hive.HiveCatalogConfig.DEFAULT_LIST_COLUMN_TYPES_SEPARATOR;

/**
 * Utils to for Hive-backed table.
 */
public class HiveTableUtil {

	private HiveTableUtil() {
	}

	/**
	 * Create a Flink's TableSchema from Hive table's columns and partition keys.
	 */
	public static TableSchema createTableSchema(List<FieldSchema> cols, List<FieldSchema> partitionKeys) {
		List<FieldSchema> allCols = new ArrayList<>(cols);
		allCols.addAll(partitionKeys);

		String[] colNames = new String[allCols.size()];
		DataType[] colTypes = new DataType[allCols.size()];

		for (int i = 0; i < allCols.size(); i++) {
			FieldSchema fs = allCols.get(i);

			colNames[i] = fs.getName();
			colTypes[i] = HiveTypeUtil.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(fs.getType()));
		}

		return TableSchema.builder()
				.fields(colNames, colTypes)
				.build();
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

	/**
	 * Create a new catalog table according to the given catalog table.
	 * The two catalog tables are the same except for their table schemas.
	 * DataTypes not supported by hive will be changed to the types that hive supports.
	 *
	 * <p>TODO:
	 * Can we just change the table schema of the given catalog table
	 * without creating a new catalog table?
	 */
	public static CatalogTable convertTableSchemaForHive(CatalogTable oldTable) {
		TableSchema oldSchema = oldTable.getSchema();
		DataType[] fieldDataTypes = oldSchema.getFieldDataTypes();
		String[] fieldNames = oldSchema.getFieldNames();

		TableSchema.Builder builder = TableSchema.builder();
		for (int i = 0; i < fieldDataTypes.length; i++) {
			// Hive stores date and timestamp using java.sql.*,
			// so we change our date / time / timestamp type here.
			switch (fieldDataTypes[i].getLogicalType().getTypeRoot()) {
				case DATE:
					builder.field(fieldNames[i], fieldDataTypes[i].bridgedTo(Date.class));
					break;
				case TIME_WITHOUT_TIME_ZONE:
					builder.field(fieldNames[i], fieldDataTypes[i].bridgedTo(Time.class));
					break;
				case TIMESTAMP_WITHOUT_TIME_ZONE:
					Preconditions.checkArgument(
						LegacyTypeInfoDataTypeConverter.canConvertToTimestampTypeInfoLenient(fieldDataTypes[i]),
						"Flink only supports timestamp with precision not larger than 3 internally");
					builder.field(fieldNames[i], fieldDataTypes[i].bridgedTo(Timestamp.class));
					break;
				default:
					builder.field(fieldNames[i], fieldDataTypes[i]);
					break;
			}
		}
		TableSchema newSchema = builder.build();

		return new CatalogTableImpl(
			newSchema,
			new ArrayList<>(oldTable.getPartitionKeys()),
			new HashMap<>(oldTable.getProperties()),
			oldTable.getComment());
	}
}
