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

package org.apache.flink.table.planner.plan.abilities.source;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsAggregatePushDown;
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.planner.functions.aggfunctions.AvgAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.CountAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.Sum0AggFunction;
import org.apache.flink.table.planner.plan.utils.AggregateInfo;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.calcite.rel.core.AggregateCall;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A sub-class of {@link SourceAbilitySpec} that can not only serialize/deserialize the aggregation
 * to/from JSON, but also can push the local aggregate into a {@link SupportsAggregatePushDown}.
 */
@JsonTypeName("AggregatePushDown")
public final class AggregatePushDownSpec extends SourceAbilitySpecBase {

    public static final String FIELD_NAME_INPUT_TYPE = "inputType";

    public static final String FIELD_NAME_GROUPING_SETS = "groupingSets";

    public static final String FIELD_NAME_AGGREGATE_CALLS = "aggregateCalls";

    @JsonProperty(FIELD_NAME_INPUT_TYPE)
    private final RowType inputType;

    @JsonProperty(FIELD_NAME_GROUPING_SETS)
    private final List<int[]> groupingSets;

    @JsonProperty(FIELD_NAME_AGGREGATE_CALLS)
    private final List<AggregateCall> aggregateCalls;

    @JsonCreator
    public AggregatePushDownSpec(
            @JsonProperty(FIELD_NAME_INPUT_TYPE) RowType inputType,
            @JsonProperty(FIELD_NAME_GROUPING_SETS) List<int[]> groupingSets,
            @JsonProperty(FIELD_NAME_AGGREGATE_CALLS) List<AggregateCall> aggregateCalls,
            @JsonProperty(FIELD_NAME_PRODUCED_TYPE) RowType producedType) {
        super(producedType);

        this.inputType = inputType;
        this.groupingSets = new ArrayList<>(checkNotNull(groupingSets));
        this.aggregateCalls = aggregateCalls;
    }

    @Override
    public void apply(DynamicTableSource tableSource, SourceAbilityContext context) {
        checkArgument(getProducedType().isPresent());
        apply(
                inputType,
                groupingSets,
                aggregateCalls,
                getProducedType().get(),
                tableSource,
                context);
    }

    @Override
    public String getDigests(SourceAbilityContext context) {
        int[] grouping = groupingSets.get(0);
        String groupingStr =
                Arrays.stream(grouping)
                        .mapToObj(index -> inputType.getFieldNames().get(index))
                        .collect(Collectors.joining(","));

        List<AggregateExpression> aggregateExpressions =
                buildAggregateExpressions(context, inputType, aggregateCalls);
        String aggFunctionsStr =
                aggregateExpressions.stream()
                        .map(AggregateExpression::asSummaryString)
                        .collect(Collectors.joining(","));

        return "aggregates=[grouping=["
                + groupingStr
                + "], aggFunctions=["
                + aggFunctionsStr
                + "]]";
    }

    public static boolean apply(
            RowType inputType,
            List<int[]> groupingSets,
            List<AggregateCall> aggregateCalls,
            RowType producedType,
            DynamicTableSource tableSource,
            SourceAbilityContext context) {
        assert context.isBatchMode() && groupingSets.size() == 1;

        List<AggregateExpression> aggregateExpressions =
                buildAggregateExpressions(context, inputType, aggregateCalls);

        if (tableSource instanceof SupportsAggregatePushDown) {
            DataType producedDataType = TypeConversions.fromLogicalToDataType(producedType);
            return ((SupportsAggregatePushDown) tableSource)
                    .applyAggregates(groupingSets, aggregateExpressions, producedDataType);
        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsAggregatePushDown.",
                            tableSource.getClass().getName()));
        }
    }

    private static List<AggregateExpression> buildAggregateExpressions(
            SourceAbilityContext context, RowType inputType, List<AggregateCall> aggregateCalls) {
        AggregateInfoList aggInfoList =
                AggregateUtil.transformToBatchAggregateInfoList(
                        context.getTypeFactory(),
                        inputType,
                        JavaScalaConversionUtil.toScala(aggregateCalls),
                        null,
                        null);
        if (aggInfoList.aggInfos().length == 0) {
            // no agg function need to be pushed down
            return Collections.emptyList();
        }

        List<AggregateExpression> aggExpressions = new ArrayList<>();
        for (AggregateInfo aggInfo : aggInfoList.aggInfos()) {
            List<FieldReferenceExpression> arguments = new ArrayList<>(1);
            for (int argIndex : aggInfo.argIndexes()) {
                DataType argType =
                        TypeConversions.fromLogicalToDataType(
                                inputType.getFields().get(argIndex).getType());
                FieldReferenceExpression field =
                        new FieldReferenceExpression(
                                inputType.getFieldNames().get(argIndex), argType, 0, argIndex);
                arguments.add(field);
            }
            if (aggInfo.function() instanceof AvgAggFunction) {
                Tuple2<Sum0AggFunction, CountAggFunction> sum0AndCountFunction =
                        AggregateUtil.deriveSumAndCountFromAvg((AvgAggFunction) aggInfo.function());
                AggregateExpression sum0Expression =
                        new AggregateExpression(
                                sum0AndCountFunction._1(),
                                arguments,
                                null,
                                aggInfo.externalResultType(),
                                aggInfo.agg().isDistinct(),
                                aggInfo.agg().isApproximate(),
                                aggInfo.agg().ignoreNulls());
                aggExpressions.add(sum0Expression);
                AggregateExpression countExpression =
                        new AggregateExpression(
                                sum0AndCountFunction._2(),
                                arguments,
                                null,
                                aggInfo.externalResultType(),
                                aggInfo.agg().isDistinct(),
                                aggInfo.agg().isApproximate(),
                                aggInfo.agg().ignoreNulls());
                aggExpressions.add(countExpression);
            } else {
                AggregateExpression aggregateExpression =
                        new AggregateExpression(
                                aggInfo.function(),
                                arguments,
                                null,
                                aggInfo.externalResultType(),
                                aggInfo.agg().isDistinct(),
                                aggInfo.agg().isApproximate(),
                                aggInfo.agg().ignoreNulls());
                aggExpressions.add(aggregateExpression);
            }
        }
        return aggExpressions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        AggregatePushDownSpec that = (AggregatePushDownSpec) o;
        return Objects.equals(inputType, that.inputType)
                && Objects.equals(groupingSets, that.groupingSets)
                && Objects.equals(aggregateCalls, that.aggregateCalls);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inputType, groupingSets, aggregateCalls);
    }

    @Override
    public boolean needAdjustFieldReferenceAfterProjection() {
        return true;
    }
}
