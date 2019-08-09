/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.Preconditions;

import java.util.UUID;

/**
 * Utility class for the operations on Table, such as
 * the operations on column name, the operations on column
 * type and so on.
 */
public class TableUtil {
	/**
	 * return a lower temp table name in lower case and
	 * use "-" as connector character.
	 *
	 * @return tableName
	 */
	public static synchronized String getTempTableName() {
		return ("temp_" + UUID.randomUUID().toString().replaceAll("-", "_"))
			.toLowerCase();
	}

	/**
	 * Find the index of <code>targetCol</code> in string
	 * array <code>tableCols</code>. It will ignore the
	 * case of the tableCols.
	 *
	 * @param tableCols a string array among which to find
	 *                  the targetCol.
	 * @param targetCol the targetCol to find.
	 * @return the index of the targetCol, if not found,
	 * returns -1.
	 */
	public static int findColIndex(String[] tableCols, String targetCol) {
		Preconditions.checkNotNull(targetCol, "targetCol is null!");
		for (int i = 0; i < tableCols.length; i++) {
			if (targetCol.equalsIgnoreCase(tableCols[i])) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * Find the indices of <code>targetCols</code> in string
	 * array <code>tableCols</code>. If <code>targetCols</code>
	 * is null, it will be replaced by the <code>tableCols</code>
	 *
	 * @param tableCols  a string array among which to
	 *                   find the targetCols.
	 * @param targetCols the targetCols to find.
	 * @return the indices of the targetCols.
	 */
	public static int[] findColIndices(String[] tableCols, String[] targetCols) {
		if (targetCols == null) {
			int[] indices = new int[tableCols.length];
			for (int i = 0; i < tableCols.length; i++) {
				indices[i] = i;
			}
			return indices;
		}
		int[] indices = new int[targetCols.length];
		for (int i = 0; i < indices.length; i++) {
			indices[i] = findColIndex(tableCols, targetCols[i]);
		}
		return indices;
	}

	/**
	 * Find the index of <code>targetCol</code> from
	 * the <code>tableSchema</code>.
	 *
	 * @param tableSchema the TableSchema among which
	 *                    to find the targetCol.
	 * @param targetCol   the targetCols to find.
	 * @return the index of the targetCol.
	 */
	public static int findColIndex(TableSchema tableSchema, String targetCol) {
		return findColIndex(tableSchema.getFieldNames(), targetCol);
	}

	/**
	 * Find the indices of <code>targetCols</code> from
	 * the <code>tableSchema</code>.
	 *
	 * @param tableSchema the TableSchema among which to
	 *                    find the targetCols.
	 * @param targetCols  the targetCols to find.
	 * @return the indices of the targetCols.
	 */
	public static int[] findColIndices(TableSchema tableSchema, String[] targetCols) {
		return findColIndices(tableSchema.getFieldNames(), targetCols);
	}

	/**
	 * Find the types of the <code>targetCols</code>.
	 * If the targetCol not exist, return null.
	 *
	 * @param tableSchema TableSchema.
	 * @param targetCols  the targetCols to find.
	 * @return the corresponding types.
	 */
	public static TypeInformation[] findColTypes(TableSchema tableSchema, String[] targetCols) {
		TypeInformation[] types = new TypeInformation[targetCols.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = findColType(tableSchema, targetCols[i]);
		}
		return types;
	}

	/**
	 * Find the type of the <code>targetCol</code>.
	 * If the targetCol not exist, return null.
	 *
	 * @param tableSchema TableSchema
	 * @param targetCol   the targetCol to find.
	 * @return the corresponding type.
	 */
	public static TypeInformation findColType(TableSchema tableSchema, String targetCol) {
		int index = findColIndex(tableSchema.getFieldNames(), targetCol);

		return index == -1 ? null : tableSchema.getFieldTypes()[index];
	}

	/**
	 * determine whether it is number type, number
	 * type includes double, long, byte, int, float
	 * and short.
	 *
	 * @param dataType the dataType to determine.
	 * @return whether it is number type
	 */
	public static boolean isNumber(TypeInformation dataType) {
		return Types.DOUBLE == dataType
			|| Types.LONG == dataType
			|| Types.BYTE == dataType
			|| Types.INT == dataType
			|| Types.FLOAT == dataType
			|| Types.SHORT == dataType;
	}

	/**
	 * determine whether it is a string type.
	 *
	 * @param dataType the dataType to determine.
	 * @return whether it is string type
	 */
	public static boolean isString(TypeInformation dataType) {
		return Types.STRING == dataType;
	}

	/**
	 * check whether <code>selectedCols</code>
	 * exist or not, if not exist, throw exception.
	 *
	 * @param tableCols    a string array among which
	 *                     to find the target selectedCols.
	 * @param selectedCols the selectedCols to assert.
	 */
	public static void assertSelectedColExist(String[] tableCols, String... selectedCols) {
		if (selectedCols != null) {
			for (String selectedCol : selectedCols) {
				if (-1 == findColIndex(tableCols, selectedCol)) {
					throw new IllegalArgumentException(" col is not exist " + selectedCol);
				}
			}
		}
	}

	/**
	 * check whether colTypes of the <code>selectedCols</code>
	 * is numerical, if not, throw exception.
	 *
	 * @param tableSchema  TableSchema
	 * @param selectedCols the selectedCols to assert.
	 */
	public static void assertNumericalCols(TableSchema tableSchema, String... selectedCols) {
		if (selectedCols != null && selectedCols.length != 0) {
			for (String selectedCol : selectedCols) {
				if (!isNumber(findColType(tableSchema, selectedCol))) {
					throw new IllegalArgumentException("col type must be number " + selectedCol);
				}
			}
		}
	}

	/**
	 * check whether colTypes of the <code>selectedCols</code>
	 * is string, if not, throw exception.
	 *
	 * @param tableSchema  TableSchema
	 * @param selectedCols the selectedCol to assert.
	 */
	public static void assertStringCols(TableSchema tableSchema, String... selectedCols) {
		if (selectedCols != null && selectedCols.length != 0) {
			for (String selectedCol : selectedCols) {
				if (!isString(findColType(tableSchema, selectedCol))) {
					throw new IllegalArgumentException("col type must be string " + selectedCol);
				}
			}
		}
	}
}
