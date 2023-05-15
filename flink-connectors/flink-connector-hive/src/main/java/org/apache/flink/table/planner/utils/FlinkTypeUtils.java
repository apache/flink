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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import static org.apache.calcite.sql.type.SqlTypeName.DAY_INTERVAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.YEAR_INTERVAL_TYPES;

/** Utils for Mapping Calcite's type to Flink's type, most code is copied from FlinkTypeFactory. */
public class FlinkTypeUtils {

    public static Class<?> getConversionClass(RelDataType relDataType) {
        // just use default conversion class
        return toLogicalType(relDataType).getDefaultConversion();
    }

    public static LogicalType toLogicalType(RelDataType relDataType) {
        SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
        if (YEAR_INTERVAL_TYPES.contains(sqlTypeName)) {
            return DataTypes.INTERVAL(DataTypes.MONTH()).getLogicalType();
        } else if (DAY_INTERVAL_TYPES.contains(sqlTypeName)) {
            if (relDataType.getPrecision() > 3) {
                throw new TableException(
                        "DAY_INTERVAL_TYPES precision is not supported:"
                                + relDataType.getPrecision());
            }
            return DataTypes.INTERVAL(DataTypes.SECOND(3)).getLogicalType();
        }
        switch (relDataType.getSqlTypeName()) {
                // todo: need to figure which sql types that hive supports
            case BOOLEAN:
                return new BooleanType();
            case TINYINT:
                return new TinyIntType();
            case SMALLINT:
                return new SmallIntType();
            case INTEGER:
                return new IntType();
            case BIGINT:
                return new BigIntType();
            case FLOAT:
                return new FloatType();
            case DOUBLE:
                return new DoubleType();
            case CHAR:
                return relDataType.getPrecision() == 0
                        ? CharType.ofEmptyLiteral()
                        : new CharType(relDataType.getPrecision());
            case VARCHAR:
                return relDataType.getPrecision() == 0
                        ? VarCharType.ofEmptyLiteral()
                        : new VarCharType(relDataType.getPrecision());
            case BINARY:
                return relDataType.getPrecision() == 0
                        ? BinaryType.ofEmptyLiteral()
                        : new BinaryType(relDataType.getPrecision());
            case VARBINARY:
                return relDataType.getPrecision() == 0
                        ? VarBinaryType.ofEmptyLiteral()
                        : new VarBinaryType(relDataType.getPrecision());
            case DECIMAL:
                return new DecimalType(relDataType.getPrecision(), relDataType.getScale());
            case TIMESTAMP:
                return new TimestampType(relDataType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new LocalZonedTimestampType(relDataType.getPrecision());
            case DATE:
                return new DateType();
            case TIME:
                return new TimeType();
            case NULL:
                return new NullType();
            case SYMBOL:
                return new SymbolType();
            case ROW:
                {
                    Preconditions.checkArgument(relDataType.isStruct());
                    return RowType.of(
                            relDataType.getFieldList().stream()
                                    .map(fieldType -> toLogicalType(fieldType.getType()))
                                    .toArray(LogicalType[]::new),
                            relDataType.getFieldNames().toArray(new String[0]));
                }
            case MULTISET:
                return new MultisetType(toLogicalType(relDataType.getComponentType()));
            case MAP:
                return new MapType(
                        toLogicalType(relDataType.getKeyType()),
                        toLogicalType(relDataType.getValueType()));
            case ARRAY:
                return new ArrayType(toLogicalType(relDataType.getComponentType()));
        }
        // shouldn't arrive in here
        throw new TableException("Type is not supported: " + relDataType.getSqlTypeName());
    }
}
