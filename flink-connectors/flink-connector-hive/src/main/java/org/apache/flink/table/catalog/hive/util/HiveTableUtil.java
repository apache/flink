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
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
				new FieldSchema(fieldNames[i], HiveTypeUtil.toHiveTypeInfo(fieldTypes[i], true).getTypeName(), null));
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

	/**
	 * Generates a filter string for partition columns from the given filter expressions.
	 *
	 * @param partColOffset The number of non-partition columns -- used to shift field reference index
	 * @param partColNames The names of all partition columns
	 * @param expressions  The filter expressions in CNF form
	 * @return an Optional filter string equivalent to the expressions, which is empty if the expressions can't be handled
	 */
	public static Optional<String> makePartitionFilter(
			int partColOffset, List<String> partColNames, List<Expression> expressions, HiveShim hiveShim) {
		List<String> filters = new ArrayList<>(expressions.size());
		ExpressionExtractor extractor = new ExpressionExtractor(partColOffset, partColNames, hiveShim);
		for (Expression expression : expressions) {
			String str = expression.accept(extractor);
			if (str == null) {
				return Optional.empty();
			}
			filters.add(str);
		}
		return Optional.of(String.join(" and ", filters));
	}

	private static class ExpressionExtractor implements ExpressionVisitor<String> {

		// maps a supported function to its name
		private static final Map<FunctionDefinition, String> FUNC_TO_STR = new HashMap<>();

		static {
			FUNC_TO_STR.put(BuiltInFunctionDefinitions.EQUALS, "=");
			FUNC_TO_STR.put(BuiltInFunctionDefinitions.NOT_EQUALS, "<>");
			FUNC_TO_STR.put(BuiltInFunctionDefinitions.GREATER_THAN, ">");
			FUNC_TO_STR.put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, ">=");
			FUNC_TO_STR.put(BuiltInFunctionDefinitions.LESS_THAN, "<");
			FUNC_TO_STR.put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, "<=");
			FUNC_TO_STR.put(BuiltInFunctionDefinitions.AND, "and");
			FUNC_TO_STR.put(BuiltInFunctionDefinitions.OR, "or");
		}

		// used to shift field reference index
		private final int partColOffset;
		private final List<String> partColNames;
		private final HiveShim hiveShim;

		ExpressionExtractor(int partColOffset, List<String> partColNames, HiveShim hiveShim) {
			this.partColOffset = partColOffset;
			this.partColNames = partColNames;
			this.hiveShim = hiveShim;
		}

		@Override
		public String visit(CallExpression call) {
			FunctionDefinition funcDef = call.getFunctionDefinition();
			if (FUNC_TO_STR.containsKey(funcDef)) {
				List<String> operands = new ArrayList<>();
				for (Expression child : call.getChildren()) {
					String operand = child.accept(this);
					if (operand == null) {
						return null;
					}
					operands.add(operand);
				}
				return "(" + String.join(" " + FUNC_TO_STR.get(funcDef) + " ", operands) + ")";
			}
			return null;
		}

		@Override
		public String visit(ValueLiteralExpression valueLiteral) {
			DataType dataType = valueLiteral.getOutputDataType();
			Object value = valueLiteral.getValueAs(Object.class).orElse(null);
			if (value == null) {
				return "null";
			}
			value = HiveInspectors.getConversion(HiveInspectors.getObjectInspector(dataType), dataType.getLogicalType(), hiveShim)
					.toHiveObject(value);
			String res = value.toString();
			LogicalTypeRoot typeRoot = dataType.getLogicalType().getTypeRoot();
			switch (typeRoot) {
				case CHAR:
				case VARCHAR:
					res = "'" + res.replace("'", "''") + "'";
					break;
				case DATE:
				case TIMESTAMP_WITHOUT_TIME_ZONE:
				case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
					// hive not support partition filter push down with these types.
					return null;
				default:
					break;
			}
			return res;
		}

		@Override
		public String visit(FieldReferenceExpression fieldReference) {
			return partColNames.get(fieldReference.getFieldIndex() - partColOffset);
		}

		@Override
		public String visit(TypeLiteralExpression typeLiteral) {
			return typeLiteral.getOutputDataType().toString();
		}

		@Override
		public String visit(Expression other) {
			// only support resolved expressions
			return null;
		}
	}

}
