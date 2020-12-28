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
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/** Utility to operator to interact with Table contents, such as rows and columns. */
public class TableUtil {
    /**
     * Return a temp table named with prefix `temp_`, follow by a random UUID.
     *
     * <p>UUID hyphens ("-") will be replaced by underscores ("_").
     *
     * @return tableName
     */
    public static synchronized String getTempTableName() {
        return ("temp_" + UUID.randomUUID().toString().replaceAll("-", "_")).toLowerCase();
    }

    /**
     * Find the index of <code>targetCol</code> in string array <code>tableCols</code>. It will
     * ignore the case of the tableCols.
     *
     * @param tableCols a string array among which to find the targetCol.
     * @param targetCol the targetCol to find.
     * @return the index of the targetCol, if not found, returns -1.
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
     * Find the index of <code>targetCol</code> from the <code>tableSchema</code>.
     *
     * @param tableSchema the TableSchema among which to find the targetCol.
     * @param targetCol the targetCols to find.
     * @return the index of the targetCol.
     */
    public static int findColIndex(TableSchema tableSchema, String targetCol) {
        return findColIndex(tableSchema.getFieldNames(), targetCol);
    }

    /**
     * Find the indices of <code>targetCols</code> in string array <code>tableCols</code>. If <code>
     * targetCols</code> is null, it will be replaced by the <code>tableCols</code>
     *
     * @param tableCols a string array among which to find the targetCols.
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
     * Find the indices of <code>targetCols</code> from the <code>tableSchema</code>.
     *
     * @param tableSchema the TableSchema among which to find the targetCols.
     * @param targetCols the targetCols to find.
     * @return the indices of the targetCols.
     */
    public static int[] findColIndices(TableSchema tableSchema, String[] targetCols) {
        return findColIndices(tableSchema.getFieldNames(), targetCols);
    }

    /**
     * Find the types of the <code>targetCols</code>. If the targetCol not exist, return null.
     *
     * @param tableSchema TableSchema.
     * @param targetCols the targetCols to find.
     * @return the corresponding types.
     */
    public static TypeInformation<?>[] findColTypes(TableSchema tableSchema, String[] targetCols) {
        if (targetCols == null) {
            return tableSchema.getFieldTypes();
        }
        TypeInformation<?>[] types = new TypeInformation[targetCols.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = findColType(tableSchema, targetCols[i]);
        }
        return types;
    }

    /**
     * Find the type of the <code>targetCol</code>. If the targetCol not exist, return null.
     *
     * @param tableSchema TableSchema
     * @param targetCol the targetCol to find.
     * @return the corresponding type.
     */
    public static TypeInformation<?> findColType(TableSchema tableSchema, String targetCol) {
        int index = findColIndex(tableSchema.getFieldNames(), targetCol);

        return index == -1 ? null : tableSchema.getFieldTypes()[index];
    }

    /**
     * Determine whether it is number type, number type includes double, long, byte, int, float and
     * short.
     *
     * @param dataType the dataType to determine.
     * @return whether it is number type
     */
    public static boolean isSupportedNumericType(TypeInformation<?> dataType) {
        return Types.DOUBLE == dataType
                || Types.LONG == dataType
                || Types.BYTE == dataType
                || Types.INT == dataType
                || Types.FLOAT == dataType
                || Types.SHORT == dataType;
    }

    /**
     * Determine whether it is a string type.
     *
     * @param dataType the dataType to determine.
     * @return whether it is string type
     */
    public static boolean isString(TypeInformation<?> dataType) {
        return Types.STRING == dataType;
    }

    /**
     * Determine whether it is a vector type.
     *
     * @param dataType the dataType to determine.
     * @return whether it is vector type
     */
    public static boolean isVector(TypeInformation<?> dataType) {
        return VectorTypes.VECTOR.equals(dataType)
                || VectorTypes.DENSE_VECTOR.equals(dataType)
                || VectorTypes.SPARSE_VECTOR.equals(dataType);
    }

    /**
     * Check whether <code>selectedCols</code> exist or not, if not exist, throw exception.
     *
     * @param tableCols a string array among which to find the target selectedCols.
     * @param selectedCols the selectedCols to assert.
     */
    public static void assertSelectedColExist(String[] tableCols, String... selectedCols) {
        if (null != selectedCols) {
            for (String selectedCol : selectedCols) {
                if (null != selectedCol) {
                    if (-1 == findColIndex(tableCols, selectedCol)) {
                        throw new IllegalArgumentException(" col is not exist " + selectedCol);
                    }
                }
            }
        }
    }

    /**
     * Check whether colTypes of the <code>selectedCols</code> is numerical, if not, throw
     * exception.
     *
     * @param tableSchema TableSchema
     * @param selectedCols the selectedCols to assert.
     */
    public static void assertNumericalCols(TableSchema tableSchema, String... selectedCols) {
        if (selectedCols != null && selectedCols.length != 0) {
            for (String selectedCol : selectedCols) {
                if (null != selectedCol) {
                    if (!isSupportedNumericType(findColType(tableSchema, selectedCol))) {
                        throw new IllegalArgumentException(
                                "col type must be number " + selectedCol);
                    }
                }
            }
        }
    }

    /**
     * Check whether colTypes of the <code>selectedCols</code> is string, if not, throw exception.
     *
     * @param tableSchema TableSchema
     * @param selectedCols the selectedCol to assert.
     */
    public static void assertStringCols(TableSchema tableSchema, String... selectedCols) {
        if (selectedCols != null && selectedCols.length != 0) {
            for (String selectedCol : selectedCols) {
                if (null != selectedCol) {
                    if (!isString(findColType(tableSchema, selectedCol))) {
                        throw new IllegalArgumentException(
                                "col type must be string " + selectedCol);
                    }
                }
            }
        }
    }

    /**
     * Check whether colTypes of the <code>selectedCols</code> is vector, if not, throw exception.
     *
     * @param tableSchema TableSchema
     * @param selectedCols the selectedCol to assert.
     * @see #isVector(TypeInformation)
     */
    public static void assertVectorCols(TableSchema tableSchema, String... selectedCols) {
        if (selectedCols != null && selectedCols.length != 0) {
            for (String selectedCol : selectedCols) {
                if (null != selectedCol) {
                    if (!isVector(findColType(tableSchema, selectedCol))) {
                        throw new IllegalArgumentException(
                                "col type must be string " + selectedCol);
                    }
                }
            }
        }
    }

    /**
     * Return the columns in the table whose types are string.
     *
     * @param tableSchema TableSchema
     * @return String columns.
     */
    public static String[] getStringCols(TableSchema tableSchema) {
        return getStringCols(tableSchema, null);
    }

    /**
     * Return the columns in the table whose types are string and are not included in the
     * excludeCols.
     *
     * <p>If <code>excludeCols</code> is null, return all the string columns.
     *
     * @param tableSchema TableSchema.
     * @param excludeCols The columns who are not considered.
     * @return string columns.
     */
    public static String[] getStringCols(TableSchema tableSchema, String[] excludeCols) {
        ArrayList<String> numericCols = new ArrayList<>();
        List<String> excludeColsList = null == excludeCols ? null : Arrays.asList(excludeCols);
        String[] inColNames = tableSchema.getFieldNames();
        TypeInformation<?>[] inColTypes = tableSchema.getFieldTypes();

        for (int i = 0; i < inColNames.length; i++) {
            if (isString(inColTypes[i])) {
                if (null == excludeColsList || !excludeColsList.contains(inColNames[i])) {
                    numericCols.add(inColNames[i]);
                }
            }
        }

        return numericCols.toArray(new String[0]);
    }

    /**
     * Return the columns in the table whose types are numeric.
     *
     * @param tableSchema TableSchema
     * @return numeric columns.
     */
    public static String[] getNumericCols(TableSchema tableSchema) {
        return getNumericCols(tableSchema, null);
    }

    /**
     * Return the columns in the table whose types are numeric and are not included in the
     * excludeCols.
     *
     * <p>If <code>excludeCols</code> is null, return all the numeric columns.
     *
     * @param tableSchema TableSchema.
     * @param excludeCols the columns who are not considered.
     * @return numeric columns.
     */
    public static String[] getNumericCols(TableSchema tableSchema, String[] excludeCols) {
        ArrayList<String> numericCols = new ArrayList<>();
        List<String> excludeColsList = (null == excludeCols ? null : Arrays.asList(excludeCols));
        String[] inColNames = tableSchema.getFieldNames();
        TypeInformation<?>[] inColTypes = tableSchema.getFieldTypes();

        for (int i = 0; i < inColNames.length; i++) {
            if (isSupportedNumericType(inColTypes[i])) {
                if (null == excludeColsList || !excludeColsList.contains(inColNames[i])) {
                    numericCols.add(inColNames[i]);
                }
            }
        }

        return numericCols.toArray(new String[0]);
    }

    /**
     * Get the columns from featureCols who are included in the <code>categoricalCols</code>, and
     * the columns whose types are string or boolean.
     *
     * <p>If <code>categoricalCols</code> is null, return all the categorical columns.
     *
     * <p>for example: In FeatureHasher which projects a number of categorical or numerical features
     * into a feature vector of a specified dimension needs to identify the categorical features.
     * And the column which is the string or boolean must be categorical. We need to find these
     * columns as categorical when user do not specify the types(categorical or numerical).
     *
     * @param tableSchema TableSchema.
     * @param featureCols the columns to chosen from.
     * @param categoricalCols the columns which are included in the final result whatever the types
     *     of them are. And it must be a subset of featureCols.
     * @return the categoricalCols.
     */
    public static String[] getCategoricalCols(
            TableSchema tableSchema, String[] featureCols, String[] categoricalCols) {
        if (null == featureCols) {
            return categoricalCols;
        }
        List<String> categoricalList =
                null == categoricalCols ? null : Arrays.asList(categoricalCols);
        List<String> featureList = Arrays.asList(featureCols);
        if (null != categoricalCols && !featureList.containsAll(categoricalList)) {
            throw new IllegalArgumentException("CategoricalCols must be included in featureCols!");
        }

        TypeInformation<?>[] featureColTypes = findColTypes(tableSchema, featureCols);
        List<String> res = new ArrayList<>();
        for (int i = 0; i < featureCols.length; i++) {
            boolean included = null != categoricalList && categoricalList.contains(featureCols[i]);
            if (included
                    || Types.BOOLEAN == featureColTypes[i]
                    || Types.STRING == featureColTypes[i]) {
                res.add(featureCols[i]);
            }
        }

        return res.toArray(new String[0]);
    }

    /** format the column names as header of markdown. */
    public static String formatTitle(String[] colNames) {
        StringBuilder sbd = new StringBuilder();
        StringBuilder sbdSplitter = new StringBuilder();

        for (int i = 0; i < colNames.length; ++i) {
            if (i > 0) {
                sbd.append("|");
                sbdSplitter.append("|");
            }

            sbd.append(colNames[i]);

            int t = null == colNames[i] ? 4 : colNames[i].length();
            for (int j = 0; j < t; j++) {
                sbdSplitter.append("-");
            }
        }

        return sbd.toString() + "\r\n" + sbdSplitter.toString();
    }

    /** format the row as body of markdown. */
    public static String formatRows(Row row) {
        StringBuilder sbd = new StringBuilder();

        for (int i = 0; i < row.getArity(); ++i) {
            if (i > 0) {
                sbd.append("|");
            }
            Object obj = row.getField(i);
            if (obj instanceof Double || obj instanceof Float) {
                sbd.append(String.format("%.4f", (double) obj));
            } else {
                sbd.append(obj);
            }
        }

        return sbd.toString();
    }

    /** format the column names and rows in table as markdown. */
    public static String format(String[] colNames, List<Row> data) {
        StringBuilder sbd = new StringBuilder();
        sbd.append(formatTitle(colNames));

        for (Row row : data) {
            sbd.append("\n").append(formatRows(row));
        }

        return sbd.toString();
    }

    /**
     * Convert column name array to SQL clause.
     *
     * <p>For example, columns "{a, b, c}" will be converted into a SQL-compatible select string
     * section: "`a`, `b`, `c`".
     *
     * @param colNames columns to convert
     * @return converted SQL clause.
     */
    public static String columnsToSqlClause(String[] colNames) {
        return Joiner.on("`,`").appendTo(new StringBuilder("`"), colNames).append("`").toString();
    }
}
