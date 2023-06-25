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

/** Utils for mapping Calcite's type to Flink's type, most code is copied from FlinkTypeFactory. */
public class FlinkTypeUtils {

    public static LogicalType toLogicalType(RelDataType relDataType) {
        SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
        boolean isNullable = relDataType.isNullable();
        if (YEAR_INTERVAL_TYPES.contains(sqlTypeName)) {
            return DataTypes.INTERVAL(DataTypes.MONTH()).getLogicalType().copy(isNullable);
        } else if (DAY_INTERVAL_TYPES.contains(sqlTypeName)) {
            if (relDataType.getPrecision() > 3) {
                throw new TableException(
                        "DAY_INTERVAL_TYPES precision is not supported:"
                                + relDataType.getPrecision());
            }
            return DataTypes.INTERVAL(DataTypes.SECOND(3)).getLogicalType().copy(isNullable);
        }
        LogicalType logicalType;
        switch (sqlTypeName) {
            case BOOLEAN:
                logicalType = new BooleanType();
                break;
            case TINYINT:
                logicalType = new TinyIntType();
                break;
            case SMALLINT:
                logicalType = new SmallIntType();
                break;
            case INTEGER:
                logicalType = new IntType();
                break;
            case BIGINT:
                logicalType = new BigIntType();
                break;
            case FLOAT:
                logicalType = new FloatType();
                break;
            case DOUBLE:
                logicalType = new DoubleType();
                break;
            case CHAR:
                logicalType =
                        relDataType.getPrecision() == 0
                                ? CharType.ofEmptyLiteral()
                                : new CharType(relDataType.getPrecision());
                break;
            case VARCHAR:
                logicalType =
                        relDataType.getPrecision() == 0
                                ? VarCharType.ofEmptyLiteral()
                                : new VarCharType(relDataType.getPrecision());
                break;
            case BINARY:
                logicalType =
                        relDataType.getPrecision() == 0
                                ? BinaryType.ofEmptyLiteral()
                                : new BinaryType(relDataType.getPrecision());
                break;
            case VARBINARY:
                logicalType =
                        relDataType.getPrecision() == 0
                                ? VarBinaryType.ofEmptyLiteral()
                                : new VarBinaryType(relDataType.getPrecision());
                break;
            case DECIMAL:
                logicalType = new DecimalType(relDataType.getPrecision(), relDataType.getScale());
                break;
            case TIMESTAMP:
                logicalType = new TimestampType(relDataType.getPrecision());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                logicalType = new LocalZonedTimestampType(relDataType.getPrecision());
                break;
            case DATE:
                logicalType = new DateType();
                break;
            case TIME:
                logicalType = new TimeType();
                break;
            case NULL:
                logicalType = new NullType();
                break;
            case SYMBOL:
                logicalType = new SymbolType();
                break;
            case ROW:
                {
                    Preconditions.checkArgument(relDataType.isStruct());
                    logicalType =
                            RowType.of(
                                    relDataType.getFieldList().stream()
                                            .map(fieldType -> toLogicalType(fieldType.getType()))
                                            .toArray(LogicalType[]::new),
                                    relDataType.getFieldNames().toArray(new String[0]));
                    break;
                }
            case MULTISET:
                logicalType = new MultisetType(toLogicalType(relDataType.getComponentType()));
                break;
            case MAP:
                logicalType =
                        new MapType(
                                toLogicalType(relDataType.getKeyType()),
                                toLogicalType(relDataType.getValueType()));
                break;
            case ARRAY:
                logicalType = new ArrayType(toLogicalType(relDataType.getComponentType()));
                break;
            default:
                // shouldn't arrive in here
                throw new TableException("Type is not supported: " + relDataType.getSqlTypeName());
        }
        return logicalType.copy(isNullable);
    }
}
