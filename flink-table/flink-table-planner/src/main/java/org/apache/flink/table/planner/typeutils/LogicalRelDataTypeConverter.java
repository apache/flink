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

package org.apache.flink.table.planner.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;
import org.apache.flink.table.planner.plan.schema.StructuredRelDataType;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Symmetric converter between {@link LogicalType} and {@link RelDataType}.
 *
 * <p>This converter has many similarities with {@link FlinkTypeFactory} and might also replace it
 * at some point. However, for now it is more consistent and does not lose information (i.e. for
 * TIME(9) or interval types). It still delegates to {@link RelDataTypeFactory} but only for
 * predefined/basic types.
 *
 * <p>Note: The conversion to {@link LogicalType} is not 100% symmetric and is currently optimized
 * for expressions. Information about the {@link StructKind} of a {@link RelRecordType} is always
 * set to {@link StructKind#PEEK_FIELDS_NO_EXPAND}. Missing precision and scale will be filled with
 * Flink's default values such that all resulting {@link LogicalType}s will be fully resolved.
 */
@Internal
public final class LogicalRelDataTypeConverter {

    public static RelDataType toRelDataType(
            LogicalType logicalType, RelDataTypeFactory relDataTypeFactory) {
        final LogicalToRelDataTypeConverter converter =
                new LogicalToRelDataTypeConverter(relDataTypeFactory);
        final RelDataType relDataType = logicalType.accept(converter);
        // this also canonizes in the factory (see SqlTypeFactoryImpl.canonize)
        return relDataTypeFactory.createTypeWithNullability(relDataType, logicalType.isNullable());
    }

    public static LogicalType toLogicalType(
            RelDataType relDataType, DataTypeFactory dataTypeFactory) {
        final LogicalType logicalType = toLogicalTypeNotNull(relDataType, dataTypeFactory);
        return logicalType.copy(relDataType.isNullable());
    }

    // --------------------------------------------------------------------------------------------
    // LogicalType to RelDataType
    // --------------------------------------------------------------------------------------------

    private static class LogicalToRelDataTypeConverter implements LogicalTypeVisitor<RelDataType> {

        private final RelDataTypeFactory relDataTypeFactory;

        LogicalToRelDataTypeConverter(RelDataTypeFactory relDataTypeFactory) {
            this.relDataTypeFactory = relDataTypeFactory;
        }

        @Override
        public RelDataType visit(CharType charType) {
            return relDataTypeFactory.createSqlType(SqlTypeName.CHAR, charType.getLength());
        }

        @Override
        public RelDataType visit(VarCharType varCharType) {
            return relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR, varCharType.getLength());
        }

        @Override
        public RelDataType visit(BooleanType booleanType) {
            return relDataTypeFactory.createSqlType(SqlTypeName.BOOLEAN);
        }

        @Override
        public RelDataType visit(BinaryType binaryType) {
            return relDataTypeFactory.createSqlType(SqlTypeName.BINARY, binaryType.getLength());
        }

        @Override
        public RelDataType visit(VarBinaryType varBinaryType) {
            return relDataTypeFactory.createSqlType(
                    SqlTypeName.VARBINARY, varBinaryType.getLength());
        }

        @Override
        public RelDataType visit(DecimalType decimalType) {
            return relDataTypeFactory.createSqlType(
                    SqlTypeName.DECIMAL, decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public RelDataType visit(TinyIntType tinyIntType) {
            return relDataTypeFactory.createSqlType(SqlTypeName.TINYINT);
        }

        @Override
        public RelDataType visit(SmallIntType smallIntType) {
            return relDataTypeFactory.createSqlType(SqlTypeName.SMALLINT);
        }

        @Override
        public RelDataType visit(IntType intType) {
            return relDataTypeFactory.createSqlType(SqlTypeName.INTEGER);
        }

        @Override
        public RelDataType visit(BigIntType bigIntType) {
            return relDataTypeFactory.createSqlType(SqlTypeName.BIGINT);
        }

        @Override
        public RelDataType visit(FloatType floatType) {
            return relDataTypeFactory.createSqlType(SqlTypeName.FLOAT);
        }

        @Override
        public RelDataType visit(DoubleType doubleType) {
            return relDataTypeFactory.createSqlType(SqlTypeName.DOUBLE);
        }

        @Override
        public RelDataType visit(DateType dateType) {
            return relDataTypeFactory.createSqlType(SqlTypeName.DATE);
        }

        @Override
        public RelDataType visit(TimeType timeType) {
            return relDataTypeFactory.createSqlType(SqlTypeName.TIME, timeType.getPrecision());
        }

        @Override
        public RelDataType visit(TimestampType timestampType) {
            final RelDataType timestampRelDataType =
                    relDataTypeFactory.createSqlType(
                            SqlTypeName.TIMESTAMP, timestampType.getPrecision());
            switch (timestampType.getKind()) {
                case REGULAR:
                    return timestampRelDataType;
                case ROWTIME:
                    assert timestampType.getPrecision() == 3;
                    return new TimeIndicatorRelDataType(
                            relDataTypeFactory.getTypeSystem(),
                            (BasicSqlType) timestampRelDataType,
                            timestampType.isNullable(),
                            true);
                default:
                    throw new TableException("Unknown timestamp kind.");
            }
        }

        @Override
        public RelDataType visit(ZonedTimestampType zonedTimestampType) {
            throw new TableException("TIMESTAMP WITH TIME ZONE is currently not supported.");
        }

        @Override
        public RelDataType visit(LocalZonedTimestampType localZonedTimestampType) {
            final RelDataType timestampRelDataType =
                    relDataTypeFactory.createSqlType(
                            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                            localZonedTimestampType.getPrecision());
            switch (localZonedTimestampType.getKind()) {
                case REGULAR:
                    return timestampRelDataType;
                case ROWTIME:
                    assert localZonedTimestampType.getPrecision() == 3;
                    return new TimeIndicatorRelDataType(
                            relDataTypeFactory.getTypeSystem(),
                            (BasicSqlType) timestampRelDataType,
                            localZonedTimestampType.isNullable(),
                            true);
                case PROCTIME:
                    assert localZonedTimestampType.getPrecision() == 3;
                    return new TimeIndicatorRelDataType(
                            relDataTypeFactory.getTypeSystem(),
                            (BasicSqlType) timestampRelDataType,
                            localZonedTimestampType.isNullable(),
                            false);
                default:
                    throw new TableException("Unknown timestamp kind.");
            }
        }

        @Override
        public RelDataType visit(YearMonthIntervalType yearMonthIntervalType) {
            final int yearPrecision = yearMonthIntervalType.getYearPrecision();
            final SqlIntervalQualifier intervalQualifier;
            switch (yearMonthIntervalType.getResolution()) {
                case YEAR:
                    intervalQualifier =
                            new SqlIntervalQualifier(
                                    TimeUnit.YEAR,
                                    yearPrecision,
                                    TimeUnit.YEAR,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    SqlParserPos.ZERO);
                    break;
                case YEAR_TO_MONTH:
                    intervalQualifier =
                            new SqlIntervalQualifier(
                                    TimeUnit.YEAR,
                                    yearPrecision,
                                    TimeUnit.MONTH,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    SqlParserPos.ZERO);
                    break;
                case MONTH:
                    intervalQualifier =
                            new SqlIntervalQualifier(
                                    TimeUnit.MONTH,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    TimeUnit.MONTH,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    SqlParserPos.ZERO);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown interval resolution.");
            }
            return relDataTypeFactory.createSqlIntervalType(intervalQualifier);
        }

        @Override
        public RelDataType visit(DayTimeIntervalType dayTimeIntervalType) {
            final int dayPrecision = dayTimeIntervalType.getDayPrecision();
            final int fractionalPrecision = dayTimeIntervalType.getFractionalPrecision();
            final SqlIntervalQualifier intervalQualifier;
            switch (dayTimeIntervalType.getResolution()) {
                case DAY:
                    intervalQualifier =
                            new SqlIntervalQualifier(
                                    TimeUnit.DAY,
                                    dayPrecision,
                                    TimeUnit.DAY,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    SqlParserPos.ZERO);
                    break;
                case DAY_TO_HOUR:
                    intervalQualifier =
                            new SqlIntervalQualifier(
                                    TimeUnit.DAY,
                                    dayPrecision,
                                    TimeUnit.HOUR,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    SqlParserPos.ZERO);
                    break;
                case DAY_TO_MINUTE:
                    intervalQualifier =
                            new SqlIntervalQualifier(
                                    TimeUnit.DAY,
                                    dayPrecision,
                                    TimeUnit.MINUTE,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    SqlParserPos.ZERO);
                    break;
                case DAY_TO_SECOND:
                    intervalQualifier =
                            new SqlIntervalQualifier(
                                    TimeUnit.DAY,
                                    dayPrecision,
                                    TimeUnit.SECOND,
                                    fractionalPrecision,
                                    SqlParserPos.ZERO);
                    break;
                case HOUR:
                    intervalQualifier =
                            new SqlIntervalQualifier(
                                    TimeUnit.HOUR,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    TimeUnit.HOUR,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    SqlParserPos.ZERO);
                    break;
                case HOUR_TO_MINUTE:
                    intervalQualifier =
                            new SqlIntervalQualifier(
                                    TimeUnit.HOUR,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    TimeUnit.MINUTE,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    SqlParserPos.ZERO);
                    break;
                case HOUR_TO_SECOND:
                    intervalQualifier =
                            new SqlIntervalQualifier(
                                    TimeUnit.HOUR,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    TimeUnit.SECOND,
                                    fractionalPrecision,
                                    SqlParserPos.ZERO);
                    break;
                case MINUTE:
                    intervalQualifier =
                            new SqlIntervalQualifier(
                                    TimeUnit.MINUTE,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    TimeUnit.MINUTE,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    SqlParserPos.ZERO);
                    break;
                case MINUTE_TO_SECOND:
                    intervalQualifier =
                            new SqlIntervalQualifier(
                                    TimeUnit.MINUTE,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    TimeUnit.SECOND,
                                    fractionalPrecision,
                                    SqlParserPos.ZERO);
                    break;
                case SECOND:
                    intervalQualifier =
                            new SqlIntervalQualifier(
                                    TimeUnit.SECOND,
                                    RelDataType.PRECISION_NOT_SPECIFIED,
                                    TimeUnit.SECOND,
                                    fractionalPrecision,
                                    SqlParserPos.ZERO);
                    break;
                default:
                    throw new TableException("Unknown interval resolution.");
            }
            return relDataTypeFactory.createSqlIntervalType(intervalQualifier);
        }

        @Override
        public RelDataType visit(ArrayType arrayType) {
            final RelDataType elementRelDataType =
                    toRelDataType(arrayType.getElementType(), relDataTypeFactory);
            return relDataTypeFactory.createArrayType(elementRelDataType, -1);
        }

        @Override
        public RelDataType visit(MultisetType multisetType) {
            final RelDataType elementRelDataType =
                    toRelDataType(multisetType.getElementType(), relDataTypeFactory);
            return relDataTypeFactory.createMultisetType(elementRelDataType, -1);
        }

        @Override
        public RelDataType visit(MapType mapType) {
            final RelDataType keyRelDataType =
                    toRelDataType(mapType.getKeyType(), relDataTypeFactory);
            final RelDataType valueRelDataType =
                    toRelDataType(mapType.getValueType(), relDataTypeFactory);
            return relDataTypeFactory.createMapType(keyRelDataType, valueRelDataType);
        }

        @Override
        public RelDataType visit(RowType rowType) {
            return relDataTypeFactory.createStructType(
                    StructKind.PEEK_FIELDS_NO_EXPAND,
                    rowType.getFields().stream()
                            .map(f -> toRelDataType(f.getType(), relDataTypeFactory))
                            .collect(Collectors.toList()),
                    rowType.getFieldNames());
        }

        @Override
        public RelDataType visit(DistinctType distinctType) {
            throw new TableException("DISTINCT type is currently not supported.");
        }

        @Override
        public RelDataType visit(StructuredType structuredType) {
            final List<RelDataTypeField> fields = new ArrayList<>();
            for (int i = 0; i < structuredType.getAttributes().size(); i++) {
                final StructuredType.StructuredAttribute attribute =
                        structuredType.getAttributes().get(i);
                final RelDataTypeField field =
                        new RelDataTypeFieldImpl(
                                attribute.getName(),
                                i,
                                toRelDataType(attribute.getType(), relDataTypeFactory));
                fields.add(field);
            }
            return new StructuredRelDataType(structuredType, fields);
        }

        @Override
        public RelDataType visit(NullType nullType) {
            return relDataTypeFactory.createSqlType(SqlTypeName.NULL);
        }

        @Override
        public RelDataType visit(RawType<?> rawType) {
            return new RawRelDataType(rawType);
        }

        @Override
        public RelDataType visit(SymbolType<?> symbolType) {
            return relDataTypeFactory.createSqlType(SqlTypeName.SYMBOL);
        }

        @Override
        public RelDataType visit(LogicalType other) {
            throw new TableException(
                    String.format(
                            "Logical type '%s' cannot be converted to a RelDataType.", other));
        }
    }

    // --------------------------------------------------------------------------------------------
    // RelDataType to LogicalType
    // --------------------------------------------------------------------------------------------

    private static LogicalType toLogicalTypeNotNull(
            RelDataType relDataType, DataTypeFactory dataTypeFactory) {
        // dataTypeFactory is a preparation for catalog user-defined types
        switch (relDataType.getSqlTypeName()) {
            case BOOLEAN:
                return new BooleanType(false);
            case TINYINT:
                return new TinyIntType(false);
            case SMALLINT:
                return new SmallIntType(false);
            case INTEGER:
                return new IntType(false);
            case BIGINT:
                return new BigIntType(false);
            case DECIMAL:
                if (relDataType.getScale() < 0) {
                    // negative scale is not supported, normalize it
                    return new DecimalType(
                            false, relDataType.getPrecision() - relDataType.getScale(), 0);
                }
                return new DecimalType(false, relDataType.getPrecision(), relDataType.getScale());
            case FLOAT:
                return new FloatType(false);
            case DOUBLE:
                return new DoubleType(false);
            case DATE:
                return new DateType(false);
            case TIME:
                return new TimeType(false, relDataType.getPrecision());
            case TIMESTAMP:
                return new TimestampType(
                        false, getTimestampKind(relDataType), relDataType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new LocalZonedTimestampType(
                        false, getTimestampKind(relDataType), relDataType.getPrecision());
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                return new YearMonthIntervalType(
                        false, getYearMonthResolution(relDataType), relDataType.getPrecision());
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
                return new DayTimeIntervalType(
                        false,
                        getDayTimeResolution(relDataType),
                        relDataType.getPrecision(),
                        relDataType.getScale());
            case INTERVAL_SECOND:
                return new DayTimeIntervalType(
                        false,
                        getDayTimeResolution(relDataType),
                        DayTimeIntervalType.DEFAULT_DAY_PRECISION,
                        relDataType.getScale());
            case CHAR:
                if (relDataType.getPrecision() == 0) {
                    return CharType.ofEmptyLiteral();
                }
                return new CharType(false, relDataType.getPrecision());
            case VARCHAR:
                if (relDataType.getPrecision() == 0) {
                    return VarCharType.ofEmptyLiteral();
                }
                return new VarCharType(false, relDataType.getPrecision());
            case BINARY:
                if (relDataType.getPrecision() == 0) {
                    return BinaryType.ofEmptyLiteral();
                }
                return new BinaryType(false, relDataType.getPrecision());
            case VARBINARY:
                if (relDataType.getPrecision() == 0) {
                    return VarBinaryType.ofEmptyLiteral();
                }
                return new VarBinaryType(false, relDataType.getPrecision());
            case NULL:
                return new NullType();
            case SYMBOL:
                return new SymbolType<>(false);
            case MULTISET:
                return new MultisetType(
                        false, toLogicalType(relDataType.getComponentType(), dataTypeFactory));
            case ARRAY:
                return new ArrayType(
                        false, toLogicalType(relDataType.getComponentType(), dataTypeFactory));
            case MAP:
                return new MapType(
                        false,
                        toLogicalType(relDataType.getKeyType(), dataTypeFactory),
                        toLogicalType(relDataType.getValueType(), dataTypeFactory));
            case DISTINCT:
                throw new TableException("DISTINCT type is currently not supported.");
            case ROW:
                return new RowType(
                        false,
                        relDataType.getFieldList().stream()
                                .map(
                                        f ->
                                                new RowField(
                                                        f.getName(),
                                                        toLogicalType(
                                                                f.getType(), dataTypeFactory)))
                                .collect(Collectors.toList()));
            case STRUCTURED:
            case OTHER:
                if (relDataType instanceof StructuredRelDataType) {
                    return ((StructuredRelDataType) relDataType).getStructuredType();
                } else if (relDataType instanceof RawRelDataType) {
                    return ((RawRelDataType) relDataType).getRawType();
                }
                // fall through
            case REAL:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case ANY:
            case CURSOR:
            case COLUMN_LIST:
            case DYNAMIC_STAR:
            case GEOMETRY:
            case SARG:
            default:
                throw new TableException("Unsupported RelDataType: " + relDataType);
        }
    }

    private static TimestampKind getTimestampKind(RelDataType relDataType) {
        if (relDataType instanceof TimeIndicatorRelDataType) {
            final TimeIndicatorRelDataType timeIndicator = (TimeIndicatorRelDataType) relDataType;
            if (timeIndicator.isEventTime()) {
                return TimestampKind.ROWTIME;
            } else {
                return TimestampKind.PROCTIME;
            }
        } else {
            return TimestampKind.REGULAR;
        }
    }

    private static YearMonthResolution getYearMonthResolution(RelDataType relDataType) {
        switch (relDataType.getSqlTypeName()) {
            case INTERVAL_YEAR:
                return YearMonthResolution.YEAR;
            case INTERVAL_YEAR_MONTH:
                return YearMonthResolution.YEAR_TO_MONTH;
            case INTERVAL_MONTH:
                return YearMonthResolution.MONTH;
            default:
                throw new TableException("Unsupported YearMonthResolution.");
        }
    }

    private static DayTimeResolution getDayTimeResolution(RelDataType relDataType) {
        switch (relDataType.getSqlTypeName()) {
            case INTERVAL_DAY:
                return DayTimeResolution.DAY;
            case INTERVAL_DAY_HOUR:
                return DayTimeResolution.DAY_TO_HOUR;
            case INTERVAL_DAY_MINUTE:
                return DayTimeResolution.DAY_TO_MINUTE;
            case INTERVAL_DAY_SECOND:
                return DayTimeResolution.DAY_TO_SECOND;
            case INTERVAL_HOUR:
                return DayTimeResolution.HOUR;
            case INTERVAL_HOUR_MINUTE:
                return DayTimeResolution.HOUR_TO_MINUTE;
            case INTERVAL_HOUR_SECOND:
                return DayTimeResolution.HOUR_TO_SECOND;
            case INTERVAL_MINUTE:
                return DayTimeResolution.MINUTE;
            case INTERVAL_MINUTE_SECOND:
                return DayTimeResolution.MINUTE_TO_SECOND;
            case INTERVAL_SECOND:
                return DayTimeResolution.SECOND;
            default:
                throw new TableException("Unsupported DayTimeResolution.");
        }
    }
}
