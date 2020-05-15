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

package org.apache.flink.connector.hbase.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Helps to specify an HBase Table's schema.
 */
@Internal
public class HBaseTableSchema implements Serializable {

	private static final long serialVersionUID = 1L;

	// A Map with key as column family.
	private final Map<String, Map<String, DataType>> familyMap = new LinkedHashMap<>();

	// information about rowkey
	private RowKeyInfo rowKeyInfo;

	// charset to parse HBase keys and strings. UTF-8 by default.
	private String charset = "UTF-8";

	/**
	 * Adds a column defined by family, qualifier, and type to the table schema.
	 *
	 * @param family    the family name
	 * @param qualifier the qualifier name
	 * @param clazz     the data type of the qualifier
	 */
	public void addColumn(String family, String qualifier, Class<?> clazz) {
		Preconditions.checkNotNull(clazz, "class type");
		DataType type = TypeConversions.fromLegacyInfoToDataType(TypeExtractor.getForClass(clazz));
		addColumn(family, qualifier, type);
	}

	public void addColumn(String family, String qualifier, DataType type) {
		Preconditions.checkNotNull(family, "family name");
		Preconditions.checkNotNull(qualifier, "qualifier name");
		Preconditions.checkNotNull(type, "data type");
		Map<String, DataType> qualifierMap = this.familyMap.get(family);

		if (!HBaseTypeUtils.isSupportedType(type.getLogicalType())) {
			// throw exception
			throw new IllegalArgumentException("Unsupported class type found " + type + ". " +
				"Better to use byte[].class and deserialize using user defined scalar functions");
		}

		if (qualifierMap == null) {
			qualifierMap = new LinkedHashMap<>();
		}
		qualifierMap.put(qualifier, type);
		familyMap.put(family, qualifierMap);
	}

	/**
	 * Sets row key information in the table schema.
	 * @param rowKeyName the row key field name
	 * @param clazz the data type of the row key
	 */
	public void setRowKey(String rowKeyName, Class<?> clazz) {
		Preconditions.checkNotNull(clazz, "row key class type");
		DataType type = TypeConversions.fromLegacyInfoToDataType(TypeExtractor.getForClass(clazz));
		setRowKey(rowKeyName, type);
	}

	public void setRowKey(String rowKeyName, DataType type) {
		Preconditions.checkNotNull(rowKeyName, "row key field name");
		Preconditions.checkNotNull(type, "row key data type");
		if (!HBaseTypeUtils.isSupportedType(type.getLogicalType())) {
			// throw exception
			throw new IllegalArgumentException("Unsupported class type found " + type + ". " +
				"Better to use byte[].class and deserialize using user defined scalar functions");
		}
		if (rowKeyInfo != null) {
			throw new IllegalArgumentException("Row key can't be set multiple times.");
		}
		this.rowKeyInfo = new RowKeyInfo(rowKeyName, type, familyMap.size());
	}

	/**
	 * Sets the charset for value strings and HBase identifiers.
	 *
	 * @param charset the charset for value strings and HBase identifiers.
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}

	/**
	 * Returns the names of all registered column families.
	 *
	 * @return The names of all registered column families.
	 */
	public String[] getFamilyNames() {
		return this.familyMap.keySet().toArray(new String[0]);
	}

	/**
	 * Returns the HBase identifiers of all registered column families.
	 *
	 * @return The HBase identifiers of all registered column families.
	 */
	public byte[][] getFamilyKeys() {
		Charset c = Charset.forName(charset);

		byte[][] familyKeys = new byte[this.familyMap.size()][];
		int i = 0;
		for (String name : this.familyMap.keySet()) {
			familyKeys[i++] = name.getBytes(c);
		}
		return familyKeys;
	}

	/**
	 * Returns the names of all registered column qualifiers of a specific column family.
	 *
	 * @param family The name of the column family for which the column qualifier names are returned.
	 * @return The names of all registered column qualifiers of a specific column family.
	 */
	public String[] getQualifierNames(String family) {
		Map<String, DataType> qualifierMap = familyMap.get(family);

		if (qualifierMap == null) {
			throw new IllegalArgumentException("Family " + family + " does not exist in schema.");
		}

		String[] qualifierNames = new String[qualifierMap.size()];
		int i = 0;
		for (String qualifier: qualifierMap.keySet()) {
			qualifierNames[i] = qualifier;
			i++;
		}
		return qualifierNames;
	}

	/**
	 * Returns the HBase identifiers of all registered column qualifiers for a specific column family.
	 *
	 * @param family The name of the column family for which the column qualifier identifiers are returned.
	 * @return The HBase identifiers of all registered column qualifiers for a specific column family.
	 */
	public byte[][] getQualifierKeys(String family) {
		Map<String, DataType> qualifierMap = familyMap.get(family);

		if (qualifierMap == null) {
			throw new IllegalArgumentException("Family " + family + " does not exist in schema.");
		}
		Charset c = Charset.forName(charset);

		byte[][] qualifierKeys = new byte[qualifierMap.size()][];
		int i = 0;
		for (String name : qualifierMap.keySet()) {
			qualifierKeys[i++] = name.getBytes(c);
		}
		return qualifierKeys;
	}

	/**
	 * Returns the types of all registered column qualifiers of a specific column family.
	 *
	 * @param family The name of the column family for which the column qualifier types are returned.
	 * @return The types of all registered column qualifiers of a specific column family.
	 */
	public TypeInformation<?>[] getQualifierTypes(String family) {
		DataType[] dataTypes = getQualifierDataTypes(family);
		return Arrays.stream(dataTypes)
			.map(TypeConversions::fromDataTypeToLegacyInfo)
			.toArray(TypeInformation[]::new);
	}

	public DataType[] getQualifierDataTypes(String family) {
		Map<String, DataType> qualifierMap = familyMap.get(family);

		if (qualifierMap == null) {
			throw new IllegalArgumentException("Family " + family + " does not exist in schema.");
		}

		DataType[] dataTypes = new DataType[qualifierMap.size()];
		int i = 0;
		for (DataType dataType : qualifierMap.values()) {
			dataTypes[i] = dataType;
			i++;
		}
		return dataTypes;
	}

	/**
	 * Returns the names and types of all registered column qualifiers of a specific column family.
	 *
	 * @param family The name of the column family for which the column qualifier names and types are returned.
	 * @return The names and types of all registered column qualifiers of a specific column family.
	 */
	private Map<String, DataType> getFamilyInfo(String family) {
		return familyMap.get(family);
	}

	/**
	 * Returns the charset for value strings and HBase identifiers.
	 *
	 * @return The charset for value strings and HBase identifiers.
	 */
	public String getStringCharset() {
		return this.charset;
	}

	/**
	 * Returns field index of row key in the table schema. Returns -1 if row key is not set.
	 */
	public int getRowKeyIndex() {
		return rowKeyInfo == null ? -1 : rowKeyInfo.rowKeyIndex;
	}

	/**
	 * Returns the optional type information of row key. Returns null if row key is not set.
	 */
	public Optional<TypeInformation<?>> getRowKeyTypeInfo() {
		return rowKeyInfo == null ?
			Optional.empty() :
			Optional.of(TypeConversions.fromDataTypeToLegacyInfo(rowKeyInfo.rowKeyType));
	}

	public Optional<DataType> getRowKeyDataType() {
		return rowKeyInfo == null ?
			Optional.empty() :
			Optional.of(rowKeyInfo.rowKeyType);
	}

	/**
	 * Returns optional value of row key name.
	 * The row key name is the field name in hbase schema which can be queried in Flink SQL.
	 */
	public Optional<String> getRowKeyName() {
		return rowKeyInfo == null ? Optional.empty() : Optional.of(rowKeyInfo.rowKeyName);
	}

	/**
	 * Gets a new hbase schema with the selected fields.
	 */
	public HBaseTableSchema getProjectedHBaseTableSchema(int[] projectedFields) {
		if (projectedFields == null) {
			return this;
		}
		HBaseTableSchema newSchema = new HBaseTableSchema();
		String[] fieldNames = convertsToTableSchema().getFieldNames();
		for (int projectedField : projectedFields) {
			String name = fieldNames[projectedField];
			if (rowKeyInfo != null && name.equals(rowKeyInfo.rowKeyName)) {
				newSchema.setRowKey(rowKeyInfo.rowKeyName, rowKeyInfo.rowKeyType);
			} else {
				Map<String, DataType> familyInfo = getFamilyInfo(name);
				for (Map.Entry<String, DataType> entry : familyInfo.entrySet()) {
					// create the newSchema
					String qualifier = entry.getKey();
					newSchema.addColumn(name, qualifier, entry.getValue());
				}
			}
		}
		newSchema.setCharset(charset);
		return newSchema;
	}

	/**
	 * Converts this {@link HBaseTableSchema} to {@link TableSchema}, the fields are consisted
	 * of families and rowkey, the order is in the definition order
	 * (i.e. calling {@link #addColumn(String, String, Class)} and {@link #setRowKey(String, Class)}).
	 * The family field is a composite type which is consisted of qualifiers.
	 *
	 * @return the {@link TableSchema} derived from the {@link HBaseTableSchema}.
	 */
	public TableSchema convertsToTableSchema() {
		String[] familyNames = getFamilyNames();
		if (rowKeyInfo != null) {
			String[] fieldNames = new String[familyNames.length + 1];
			DataType[] fieldTypes = new DataType[familyNames.length + 1];
			for (int i = 0; i < fieldNames.length; i++) {
				if (i == rowKeyInfo.rowKeyIndex) {
					fieldNames[i] = rowKeyInfo.rowKeyName;
					fieldTypes[i] = rowKeyInfo.rowKeyType;
				} else {
					int familyIndex = i < rowKeyInfo.rowKeyIndex ? i : i - 1;
					String family = familyNames[familyIndex];
					fieldNames[i] = family;
					fieldTypes[i] = TableSchema.builder()
						.fields(getQualifierNames(family), getQualifierDataTypes(family))
						.build()
						.toRowDataType();
				}
			}
			return TableSchema.builder().fields(fieldNames, fieldTypes).build();
		} else {
			String[] fieldNames = new String[familyNames.length];
			DataType[] fieldTypes = new DataType[familyNames.length];
			for (int i = 0; i < fieldNames.length; i++) {
				String family = familyNames[i];
				fieldNames[i] = family;
				fieldTypes[i] = TableSchema.builder()
					.fields(getQualifierNames(family), getQualifierDataTypes(family))
					.build()
					.toRowDataType();
			}
			return TableSchema.builder().fields(fieldNames, fieldTypes).build();
		}
	}

	/**
	 * Construct a {@link HBaseTableSchema} from a {@link TableSchema}.
	 */
	public static HBaseTableSchema fromTableSchema(TableSchema schema) {
		HBaseTableSchema hbaseSchema = new HBaseTableSchema();
		RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
		for (RowType.RowField field : rowType.getFields()) {
			LogicalType fieldType = field.getType();
			if (fieldType.getTypeRoot() == LogicalTypeRoot.ROW) {
				RowType familyType = (RowType) fieldType;
				String familyName = field.getName();
				for (RowType.RowField qualifier : familyType.getFields()) {
					hbaseSchema.addColumn(
						familyName,
						qualifier.getName(),
						fromLogicalToDataType(qualifier.getType()));
				}
			} else if (fieldType.getChildren().size() == 0) {
				hbaseSchema.setRowKey(field.getName(), fromLogicalToDataType(fieldType));
			} else {
				throw new IllegalArgumentException(
					"Unsupported field type '" + fieldType + "' for HBase.");
			}
		}
		schema.getPrimaryKey().ifPresent(k -> {
			if (k.getColumns().size() > 1 ||
					!hbaseSchema.getRowKeyName().isPresent() ||
					!hbaseSchema.getRowKeyName().get().equals(k.getColumns().get(0))) {
				throw new IllegalArgumentException(
					"Primary Key of HBase table should only be defined on the row key field.");
			}
		});
		if (!hbaseSchema.getRowKeyName().isPresent()) {
			throw new IllegalArgumentException(
				"HBase table requires to define a row key field. A row key field must be an atomic type.");
		}
		return hbaseSchema;
	}

	// ------------------------------------------------------------------------------------

	/**
	 * An class contains information about rowKey, such as rowKeyName, rowKeyType, rowKeyIndex.
	 */
	private static class RowKeyInfo implements Serializable {
		private static final long serialVersionUID = 1L;
		final String rowKeyName;
		final DataType rowKeyType;
		final int rowKeyIndex;

		RowKeyInfo(String rowKeyName, DataType rowKeyType, int rowKeyIndex) {
			this.rowKeyName = rowKeyName;
			this.rowKeyType = rowKeyType;
			this.rowKeyIndex = rowKeyIndex;
		}
	}
}
