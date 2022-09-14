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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGenException;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;
import java.time.ZoneOffset;

import static org.apache.flink.table.planner.utils.TimestampStringUtils.toLocalDateTime;

/** Utilities to work with {@link RexLiteral}. */
@Internal
public class RexLiteralUtil {

    private RexLiteralUtil() {}

    /** See {@link #toFlinkInternalValue(Comparable, LogicalType)}. */
    public static Tuple2<Object, LogicalType> toFlinkInternalValue(RexLiteral literal) {
        LogicalType targetType = FlinkTypeFactory.toLogicalType(literal.getType());
        return Tuple2.of(
                toFlinkInternalValue(literal.getValueAs(Comparable.class), targetType), targetType);
    }

    /**
     * Convert a value from Calcite's {@link Comparable} data structures to Flink internal data
     * structures and also tries to be a bit flexible by accepting usual Java types such as String
     * and boxed numerics.
     *
     * <p>In case of symbol types, this function will return provided value, checking that it's an
     * {@link Enum}.
     *
     * <p>This function is essentially like {@link FlinkTypeFactory#toLogicalType(RelDataType)} but
     * for values.
     *
     * <p>Check {@link RexLiteral#valueMatchesType(Comparable, SqlTypeName, boolean)} for details on
     * the {@link Comparable} data structures and {@link org.apache.flink.table.data.RowData} for
     * details on Flink's internal data structures.
     *
     * @param value the value in Calcite's {@link Comparable} data structures
     * @param valueType the type of the value
     * @return the value in Flink's internal data structures
     * @throws IllegalArgumentException in case the class of value does not match the expectations
     *     of valueType
     */
    public static Object toFlinkInternalValue(Comparable<?> value, LogicalType valueType) {
        if (value == null) {
            return null;
        }
        switch (valueType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                if (value instanceof NlsString) {
                    return BinaryStringData.fromString(((NlsString) value).getValue());
                }
                if (value instanceof String) {
                    return BinaryStringData.fromString((String) value);
                }
                break;
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                break;
            case BINARY:
            case VARBINARY:
                if (value instanceof ByteString) {
                    return ((ByteString) value).getBytes();
                }
                break;
            case DECIMAL:
                if (value instanceof BigDecimal) {
                    return DecimalData.fromBigDecimal(
                            (BigDecimal) value,
                            LogicalTypeChecks.getPrecision(valueType),
                            LogicalTypeChecks.getScale(valueType));
                }
                break;
            case TINYINT:
                if (value instanceof Number) {
                    return ((Number) value).byteValue();
                }
                break;
            case SMALLINT:
                if (value instanceof Number) {
                    return ((Number) value).shortValue();
                }
                break;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                break;
            case FLOAT:
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                }
                break;
            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                break;
            case DATE:
                if (value instanceof DateString) {
                    return ((DateString) value).getDaysSinceEpoch();
                }
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                break;
            case TIME_WITHOUT_TIME_ZONE:
                if (value instanceof TimeString) {
                    return ((TimeString) value).getMillisOfDay();
                }
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (value instanceof TimestampString) {
                    return TimestampData.fromLocalDateTime(
                            toLocalDateTime((TimestampString) value));
                }
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (value instanceof TimestampString) {
                    return TimestampData.fromInstant(
                            toLocalDateTime((TimestampString) value)
                                    .atOffset(ZoneOffset.UTC)
                                    .toInstant());
                }
                break;
            case DISTINCT_TYPE:
                return toFlinkInternalValue(value, ((DistinctType) valueType).getSourceType());
            case SYMBOL:
                if (value instanceof Enum) {
                    return value;
                }
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
            case ARRAY:
            case MULTISET:
            case MAP:
            case ROW:
            case STRUCTURED_TYPE:
            case NULL:
            case UNRESOLVED:
                throw new CodeGenException("Type not supported: " + valueType);
        }
        throw new IllegalStateException(
                "Unexpected class " + value.getClass() + " for value of type " + valueType);
    }
}
