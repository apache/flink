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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.catalog.hive.HiveTableConfig.DEFAULT_LIST_COLUMN_TYPES_SEPARATOR;

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
		TypeInformation[] colTypes = new TypeInformation[allCols.size()];

		for (int i = 0; i < allCols.size(); i++) {
			FieldSchema fs = allCols.get(i);

			colNames[i] = fs.getName();
			colTypes[i] = HiveTypeUtil.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(fs.getType()));
		}

		return new TableSchema(colNames, colTypes);
	}

	/**
	 * Create Hive columns from Flink TableSchema.
	 */
	public static List<FieldSchema> createHiveColumns(TableSchema schema) {
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
	 * Convert partition value string to actual type in flink.
	 * @param partitionValue
	 * @param clazz
	 * @return
	 */
	//todo: check whether hive partition field can be decimal or not .
	public static Object getActualObjectFromString(String partitionValue, Class clazz) {
		Object partitionObject = null;
		if (String.class == clazz) {
			partitionObject = partitionValue;
		} else if (Short.class == clazz) {
			partitionObject = Short.parseShort(partitionValue);
		} else if (Integer.class == clazz) {
			partitionObject = Integer.parseInt(partitionValue);
		} else if (Long.class == clazz) {
			partitionObject = Long.parseLong(partitionValue);
		} else if (Float.class == clazz) {
			partitionObject = Float.parseFloat(partitionValue);
		} else if (Double.class == clazz) {
			partitionObject = Double.parseDouble(partitionValue);
		} else if (Boolean.class == clazz) {
			partitionObject = Boolean.parseBoolean(partitionValue);
		} else if (Timestamp.class == clazz) {
			partitionObject = Timestamp.parse(partitionValue);
		} else if (Date.class == clazz) {
			partitionObject = Date.parse(partitionValue);
		} else if (Time.class == clazz) {
			partitionObject = Time.parse(partitionValue);
		} else if (BigDecimal.class == clazz) {
			partitionObject = new BigDecimal(partitionValue);
		} else if (BigInteger.class == clazz) {
			partitionObject = new BigInteger(partitionValue);
		}
		return partitionObject;
	}

	// --------------------------------------------------------------------------------------------
	//  Helper methods
	// --------------------------------------------------------------------------------------------

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
		properties.setProperty(serdeConstants.LIST_COLUMNS, StringUtils.join(colNames, ","));
		// Note: serdeConstants.COLUMN_NAME_DELIMITER is not defined in previous Hive. We use a literal to save on shim
		properties.setProperty("column.name.delimite", String.valueOf(SerDeUtils.COMMA));
		properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, StringUtils.join(colTypes, DEFAULT_LIST_COLUMN_TYPES_SEPARATOR));
		properties.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
		properties.putAll(parameters);
		return properties;
	}

}
