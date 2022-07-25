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

package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;

import org.postgresql.jdbc.PgArray;
import org.postgresql.util.PGobject;

import java.lang.reflect.Array;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * PostgreSQL.
 */
public class PostgresRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;
    private static final Pattern UUID_REGEX_PATTERN =
            Pattern.compile("^[{]?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}[}]?$");

    @Override
    public String converterName() {
        return "PostgreSQL";
    }

    public PostgresRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();

        if (root == LogicalTypeRoot.ARRAY) {
            ArrayType arrayType = (ArrayType) type;
            return createPostgresArrayConverter(arrayType);
        } else {
            return createPrimitiveConverter(type);
        }
    }

    @Override
    protected JdbcSerializationConverter createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return (val, index, statement) -> {
                    String valString = val.getString(index).toString();

                    if (UUID_REGEX_PATTERN.matcher(valString).matches()) {
                        statement.setObject(index, UUID.fromString(valString));
                    } else {
                        statement.setString(index, valString);
                    }
                };
        }

        return super.createExternalConverter(type);
    }

    @Override
    protected JdbcSerializationConverter createNullableExternalConverter(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        if (root == LogicalTypeRoot.ARRAY) {
            return createPostgresArraySerializationConverter(type);
        } else {
            return super.createNullableExternalConverter(type);
        }
    }

    private JdbcSerializationConverter createPostgresArraySerializationConverter(LogicalType type) {
        return (val, index, statement) -> {
            ArrayData arrayData = val.getArray(index);
            LogicalTypeRoot arrayType = type.getChildren().get(0).getTypeRoot();

            int sqlType =
                    JdbcTypeUtil.typeInformationToSqlType(
                            TypeConversions.fromDataTypeToLegacyInfo(
                                    TypeConversions.fromLogicalToDataType(type)));

            if (val == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                statement.setNull(index, sqlType);
            } else {
                switch (arrayType) {
                    case TINYINT:
                        byte[] byteArray = new byte[arrayData.size()];
                        for (int i = 0; i < byteArray.length; i++) {
                            byteArray[i] = arrayData.getByte(i);
                        }
                        statement.setObject(index, byteArray);
                        break;
                    case SMALLINT:
                        short[] shortArray = new short[arrayData.size()];
                        for (int i = 0; i < shortArray.length; i++) {
                            shortArray[i] = arrayData.getShort(i);
                        }
                        statement.setObject(index, shortArray);
                        break;
                    case INTEGER:
                        int[] intArray = new int[arrayData.size()];
                        for (int i = 0; i < intArray.length; i++) {
                            intArray[i] = arrayData.getInt(i);
                        }
                        statement.setObject(index, intArray);
                        break;
                    case BIGINT:
                    case INTERVAL_DAY_TIME:
                        long[] longArray = new long[arrayData.size()];
                        for (int i = 0; i < longArray.length; i++) {
                            longArray[i] = arrayData.getLong(i);
                        }
                        statement.setObject(index, longArray);
                        break;
                    case FLOAT:
                        float[] floatArray = new float[arrayData.size()];
                        for (int i = 0; i < floatArray.length; i++) {
                            floatArray[i] = arrayData.getFloat(i);
                        }
                        statement.setObject(index, floatArray);
                        break;
                    case DOUBLE:
                        double[] doubleArray = new double[arrayData.size()];
                        for (int i = 0; i < doubleArray.length; i++) {
                            doubleArray[i] = arrayData.getDouble(i);
                        }
                        statement.setObject(index, doubleArray);
                        break;
                    case CHAR:
                    case VARCHAR:
                        String[] stringArray = new String[arrayData.size()];
                        for (int i = 0; i < stringArray.length; i++) {
                            stringArray[i] = arrayData.getString(i).toString();
                        }
                        statement.setObject(index, stringArray);
                        break;
                    case BINARY:
                    case VARBINARY:
                        byte[][] binaryArray = new byte[arrayData.size()][];
                        for (int i = 0; i < binaryArray.length; i++) {
                            binaryArray[i] = arrayData.getBinary(i);
                        }
                        statement.setObject(index, binaryArray);
                        break;
                    case DECIMAL:
                    case DATE:
                    case TIME_WITHOUT_TIME_ZONE:
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    case ARRAY:
                    case ROW:
                    case MAP:
                    case MULTISET:
                    case RAW:
                    default:
                        throw new IllegalStateException(
                                String.format(
                                        "Writing ARRAY<%s> type is not yet supported in JDBC:%s.",
                                        arrayType, converterName()));
                }
            }
        };
    }

    private JdbcDeserializationConverter createPostgresArrayConverter(ArrayType arrayType) {
        // PG's bytea[] is wrapped in PGobject, rather than primitive byte arrays
        if (LogicalTypeChecks.hasFamily(
                arrayType.getElementType(), LogicalTypeFamily.BINARY_STRING)) {
            final Class<?> elementClass =
                    LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
            final JdbcDeserializationConverter elementConverter =
                    createNullableInternalConverter(arrayType.getElementType());

            return val -> {
                PgArray pgArray = (PgArray) val;
                Object[] in = (Object[]) pgArray.getArray();
                final Object[] array = (Object[]) Array.newInstance(elementClass, in.length);
                for (int i = 0; i < in.length; i++) {
                    array[i] =
                            elementConverter.deserialize(((PGobject) in[i]).getValue().getBytes());
                }
                return new GenericArrayData(array);
            };
        } else {
            final Class<?> elementClass =
                    LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
            final JdbcDeserializationConverter elementConverter =
                    createNullableInternalConverter(arrayType.getElementType());
            return val -> {
                PgArray pgArray = (PgArray) val;
                Object[] in = (Object[]) pgArray.getArray();
                final Object[] array = (Object[]) Array.newInstance(elementClass, in.length);
                for (int i = 0; i < in.length; i++) {
                    array[i] = elementConverter.deserialize(in[i]);
                }
                return new GenericArrayData(array);
            };
        }
    }

    // Have its own method so that Postgres can support primitives that super class doesn't support
    // in the future
    private JdbcDeserializationConverter createPrimitiveConverter(LogicalType type) {
        return super.createInternalConverter(type);
    }
}
