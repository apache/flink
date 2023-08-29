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

package org.apache.flink.table.planner.expressions.converter;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.NestedFieldReferenceExpression;
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.expressions.TimePointUnit;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexFieldVariable;
import org.apache.flink.table.planner.expressions.RexNodeExpression;
import org.apache.flink.table.planner.expressions.converter.CallExpressionConvertRule.ConvertContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimeType;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.typeutils.SymbolUtil.commonToCalcite;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;
import static org.apache.flink.table.planner.utils.TimestampStringUtils.fromLocalDateTime;
import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;

/** Visit expression to generator {@link RexNode}. */
public class ExpressionConverter implements ExpressionVisitor<RexNode> {

    private final RelBuilder relBuilder;
    private final FlinkTypeFactory typeFactory;
    private final DataTypeFactory dataTypeFactory;

    public ExpressionConverter(RelBuilder relBuilder) {
        this.relBuilder = relBuilder;
        this.typeFactory = (FlinkTypeFactory) relBuilder.getRexBuilder().getTypeFactory();
        this.dataTypeFactory =
                unwrapContext(relBuilder.getCluster()).getCatalogManager().getDataTypeFactory();
    }

    private List<CallExpressionConvertRule> getFunctionConvertChain(boolean isBatchMode) {
        return Arrays.asList(
                new LegacyScalarFunctionConvertRule(),
                new FunctionDefinitionConvertRule(),
                new OverConvertRule(),
                DirectConvertRule.instance(isBatchMode),
                new CustomizedConvertRule());
    }

    @Override
    public RexNode visit(CallExpression call) {
        boolean isBatchMode = unwrapContext(relBuilder).isBatchMode();
        for (CallExpressionConvertRule rule : getFunctionConvertChain(isBatchMode)) {
            Optional<RexNode> converted = rule.convert(call, newFunctionContext());
            if (converted.isPresent()) {
                return converted.get();
            }
        }
        throw new RuntimeException("Unknown call expression: " + call);
    }

    @Override
    public RexNode visit(ValueLiteralExpression valueLiteral) {
        LogicalType type = fromDataTypeToLogicalType(valueLiteral.getOutputDataType());
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        FlinkTypeFactory typeFactory = (FlinkTypeFactory) relBuilder.getTypeFactory();

        RelDataType relDataType = typeFactory.createFieldTypeFromLogicalType(type);

        if (valueLiteral.isNull()) {
            return rexBuilder.makeNullLiteral(relDataType);
        }

        Object value = null;
        switch (type.getTypeRoot()) {
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                value = extractValue(valueLiteral, BigDecimal.class);
                break;
            case VARCHAR:
            case CHAR:
                value = extractValue(valueLiteral, String.class);
                break;
            case BINARY:
            case VARBINARY:
                value = new ByteString(extractValue(valueLiteral, byte[].class));
                break;
            case INTERVAL_YEAR_MONTH:
                // convert to total months
                value =
                        BigDecimal.valueOf(
                                extractValue(valueLiteral, Period.class).toTotalMonths());
                break;
            case INTERVAL_DAY_TIME:
                // TODO planner supports only milliseconds precision
                // convert to total millis
                value = BigDecimal.valueOf(extractValue(valueLiteral, Duration.class).toMillis());
                break;
            case DATE:
                value =
                        DateString.fromDaysSinceEpoch(
                                (int) extractValue(valueLiteral, LocalDate.class).toEpochDay());
                break;
            case TIME_WITHOUT_TIME_ZONE:
                // TODO type factory strips the precision, for literals we can be more lenient
                // already
                // Moreover conversion from long supports precision up to TIME(3) planner does not
                // support higher
                // precisions
                TimeType timeType = (TimeType) type;
                int precision = timeType.getPrecision();
                relDataType = typeFactory.createSqlType(SqlTypeName.TIME, Math.min(precision, 3));
                value =
                        TimeString.fromMillisOfDay(
                                extractValue(valueLiteral, LocalTime.class)
                                        .get(ChronoField.MILLI_OF_DAY));
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                LocalDateTime datetime = extractValue(valueLiteral, LocalDateTime.class);
                value = fromLocalDateTime(datetime);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // normalize to UTC
                Instant instant = extractValue(valueLiteral, Instant.class);
                value = fromLocalDateTime(instant.atOffset(ZoneOffset.UTC).toLocalDateTime());
                break;
            default:
                value = extractValue(valueLiteral, Object.class);
                if (value instanceof TimePointUnit) {
                    value = commonToCalcite((TimePointUnit) value);
                } else if (value instanceof TimeIntervalUnit) {
                    value = commonToCalcite((TimeIntervalUnit) value);
                }
                break;
        }

        return rexBuilder.makeLiteral(
                value,
                relDataType,
                // This flag ensures that the resulting RexNode will match the type of the literal.
                // It might be wrapped with a CAST though, if the value requires adjusting. In
                // majority of
                // cases the type will be simply pushed down into the RexLiteral, see
                // RexBuilder#makeCast.
                true);
    }

    @Override
    public RexNode visit(FieldReferenceExpression fieldReference) {
        // We can not use inputCount+inputIndex+FieldIndex to construct field of calcite.
        // See QueryOperationConverter.SingleRelVisitor.visit(AggregateQueryOperation).
        // Calcite will shuffle the output order of groupings.
        // So the output fields order will be changed too.
        // See RelBuilder.aggregate, it use ImmutableBitSet to store groupings,
        return relBuilder.field(fieldReference.getName());
    }

    @Override
    public RexNode visit(NestedFieldReferenceExpression nestedFieldReference) {
        String[] fieldNames = nestedFieldReference.getFieldNames();
        RexNode fieldAccess = relBuilder.field(fieldNames[0]);
        for (int i = 1; i < fieldNames.length; i++) {
            fieldAccess =
                    relBuilder.getRexBuilder().makeFieldAccess(fieldAccess, fieldNames[i], true);
        }
        return fieldAccess;
    }

    @Override
    public RexNode visit(TypeLiteralExpression typeLiteral) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RexNode visit(Expression other) {
        if (other instanceof RexNodeExpression) {
            return ((RexNodeExpression) other).getRexNode();
        } else if (other instanceof LocalReferenceExpression) {
            final LocalReferenceExpression local = (LocalReferenceExpression) other;
            // check whether the local field reference can actually be resolved to an existing
            // field otherwise preserve the locality attribute
            RelNode inputNode;
            try {
                inputNode = relBuilder.peek();
            } catch (Throwable t) {
                inputNode = null;
            }
            if (inputNode != null
                    && inputNode.getRowType().getFieldNames().contains(local.getName())) {
                return relBuilder.field(local.getName());
            }
            return new RexFieldVariable(
                    local.getName(),
                    typeFactory.createFieldTypeFromLogicalType(
                            fromDataTypeToLogicalType(local.getOutputDataType())));
        } else {
            throw new UnsupportedOperationException(
                    other.getClass().getSimpleName() + ":" + other.toString());
        }
    }

    public static List<RexNode> toRexNodes(ConvertContext context, List<Expression> expr) {
        return expr.stream().map(context::toRexNode).collect(Collectors.toList());
    }

    private ConvertContext newFunctionContext() {
        return new ConvertContext() {
            @Override
            public RexNode toRexNode(Expression expr) {
                return expr.accept(ExpressionConverter.this);
            }

            @Override
            public RelBuilder getRelBuilder() {
                return relBuilder;
            }

            @Override
            public FlinkTypeFactory getTypeFactory() {
                return typeFactory;
            }

            @Override
            public DataTypeFactory getDataTypeFactory() {
                return dataTypeFactory;
            }
        };
    }

    /**
     * Extracts a value from a literal. Including planner-specific instances such as {@link
     * DecimalData}.
     */
    @SuppressWarnings("unchecked")
    public static <T> T extractValue(ValueLiteralExpression literal, Class<T> clazz) {
        final Optional<Object> possibleObject = literal.getValueAs(Object.class);
        if (!possibleObject.isPresent()) {
            throw new TableException("Invalid literal.");
        }
        final Object object = possibleObject.get();

        if (clazz.equals(BigDecimal.class)) {
            final Optional<BigDecimal> possibleDecimal = literal.getValueAs(BigDecimal.class);
            if (possibleDecimal.isPresent()) {
                return (T) possibleDecimal.get();
            }
            if (object instanceof DecimalData) {
                return (T) ((DecimalData) object).toBigDecimal();
            }
        }

        return literal.getValueAs(clazz)
                .orElseThrow(() -> new TableException("Unsupported literal class: " + clazz));
    }
}
