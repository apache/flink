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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.calcite.FlinkRelFactories.ExpandFactory;
import org.apache.flink.table.planner.calcite.FlinkRelFactories.RankFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.plan.QueryOperationConverter;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalTableAggregate;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowTableAggregate;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.plan.utils.AggregateUtil.isTableAggregate;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;

/** Flink-specific {@link RelBuilder}. */
@Internal
public final class FlinkRelBuilder extends RelBuilder {

    public static final RelBuilder.Config FLINK_REL_BUILDER_CONFIG =
            Config.DEFAULT.withSimplifyValues(false);
    private final QueryOperationConverter toRelNodeConverter;

    private final ExpandFactory expandFactory;

    private final RankFactory rankFactory;

    private FlinkRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);

        this.toRelNodeConverter =
                new QueryOperationConverter(this, unwrapContext(context).isBatchMode());
        this.expandFactory =
                Util.first(
                        context.unwrap(ExpandFactory.class),
                        FlinkRelFactories.DEFAULT_EXPAND_FACTORY());
        this.rankFactory =
                Util.first(
                        context.unwrap(RankFactory.class),
                        FlinkRelFactories.DEFAULT_RANK_FACTORY());
    }

    public static FlinkRelBuilder of(
            Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        return new FlinkRelBuilder(Preconditions.checkNotNull(context), cluster, relOptSchema);
    }

    public static FlinkRelBuilder of(RelOptCluster cluster, RelOptSchema relOptSchema) {
        return FlinkRelBuilder.of(cluster.getPlanner().getContext(), cluster, relOptSchema);
    }

    public static RelBuilderFactory proto(Context context) {
        return (cluster, schema) -> {
            final Context clusterContext = cluster.getPlanner().getContext();
            final Context chain = Contexts.chain(context, clusterContext);
            return FlinkRelBuilder.of(chain, cluster, schema);
        };
    }

    /**
     * {@link RelBuilder#functionScan(SqlOperator, int, Iterable)} cannot work smoothly with aliases
     * which is why we implement a custom one. The method is static because some {@link RelOptRule}s
     * don't use {@link FlinkRelBuilder}.
     */
    public static RelBuilder pushFunctionScan(
            RelBuilder relBuilder,
            SqlOperator operator,
            int inputCount,
            Iterable<RexNode> operands,
            List<String> aliases) {
        Preconditions.checkArgument(
                operator instanceof BridgingSqlFunction.WithTableFunction,
                "Table function expected.");
        final RexBuilder rexBuilder = relBuilder.getRexBuilder();
        final RelDataTypeFactory typeFactory = relBuilder.getTypeFactory();

        final List<RelNode> inputs = new LinkedList<>();
        for (int i = 0; i < inputCount; i++) {
            inputs.add(0, relBuilder.build());
        }

        final List<RexNode> operandList = CollectionUtil.iterableToList(operands);

        final RelDataType functionRelDataType = rexBuilder.deriveReturnType(operator, operandList);
        final List<RelDataType> fieldRelDataTypes;
        if (functionRelDataType.isStruct()) {
            fieldRelDataTypes =
                    functionRelDataType.getFieldList().stream()
                            .map(RelDataTypeField::getType)
                            .collect(Collectors.toList());
        } else {
            fieldRelDataTypes = Collections.singletonList(functionRelDataType);
        }
        final RelDataType rowRelDataType = typeFactory.createStructType(fieldRelDataTypes, aliases);

        final RexNode call = rexBuilder.makeCall(rowRelDataType, operator, operandList);
        final RelNode functionScan =
                LogicalTableFunctionScan.create(
                        relBuilder.getCluster(),
                        inputs,
                        call,
                        null,
                        rowRelDataType,
                        Collections.emptySet());
        return relBuilder.push(functionScan);
    }

    public RelBuilder expand(List<List<RexNode>> projects, int expandIdIndex) {
        final RelNode input = build();
        final RelNode expand = expandFactory.createExpand(input, projects, expandIdIndex);
        return push(expand);
    }

    public RelBuilder rank(
            ImmutableBitSet partitionKey,
            RelCollation orderKey,
            RankType rankType,
            RankRange rankRange,
            RelDataTypeField rankNumberType,
            boolean outputRankNumber) {
        final RelNode input = build();
        final RelNode rank =
                rankFactory.createRank(
                        input,
                        partitionKey,
                        orderKey,
                        rankType,
                        rankRange,
                        rankNumberType,
                        outputRankNumber);
        return push(rank);
    }

    /** Build non-window aggregate for either aggregate or table aggregate. */
    @Override
    public RelBuilder aggregate(
            RelBuilder.GroupKey groupKey, Iterable<RelBuilder.AggCall> aggCalls) {
        // build a relNode, the build() may also return a project
        RelNode relNode = super.aggregate(groupKey, aggCalls).build();

        if (relNode instanceof LogicalAggregate) {
            final LogicalAggregate logicalAggregate = (LogicalAggregate) relNode;
            if (isTableAggregate(logicalAggregate.getAggCallList())) {
                relNode = LogicalTableAggregate.create(logicalAggregate);
            } else if (isCountStarAgg(logicalAggregate)) {
                final RelNode newAggInput =
                        push(logicalAggregate.getInput(0)).project(literal(0)).build();
                relNode =
                        logicalAggregate.copy(
                                logicalAggregate.getTraitSet(), ImmutableList.of(newAggInput));
            }
        }

        return push(relNode);
    }

    /** Build window aggregate for either aggregate or table aggregate. */
    public RelBuilder windowAggregate(
            LogicalWindow window,
            GroupKey groupKey,
            List<NamedWindowProperty> namedProperties,
            Iterable<AggCall> aggCalls) {
        // build logical aggregate

        // Because of:
        // [CALCITE-3763] RelBuilder.aggregate should prune unused fields from the input,
        // if the input is a Project.
        //
        // the field can not be pruned if it is referenced by other expressions
        // of the window aggregation(i.e. the TUMBLE_START/END).
        // To solve this, we config the RelBuilder to forbidden this feature.
        final LogicalAggregate aggregate =
                (LogicalAggregate)
                        super.transform(t -> t.withPruneInputOfAggregate(false))
                                .push(build())
                                .aggregate(groupKey, aggCalls)
                                .build();

        // build logical window aggregate from it
        final RelNode windowAggregate;
        if (isTableAggregate(aggregate.getAggCallList())) {
            windowAggregate =
                    LogicalWindowTableAggregate.create(window, namedProperties, aggregate);
        } else {
            windowAggregate = LogicalWindowAggregate.create(window, namedProperties, aggregate);
        }
        return push(windowAggregate);
    }

    /** Build watermark assigner relational node. */
    public RelBuilder watermark(int rowtimeFieldIndex, RexNode watermarkExpr) {
        final RelNode input = build();
        final RelNode relNode =
                LogicalWatermarkAssigner.create(cluster, input, rowtimeFieldIndex, watermarkExpr);
        return push(relNode);
    }

    public RelBuilder queryOperation(QueryOperation queryOperation) {
        final RelNode relNode = queryOperation.accept(toRelNodeConverter);
        return push(relNode);
    }

    public RelBuilder scan(ObjectIdentifier identifier, Map<String, String> dynamicOptions) {
        final List<RelHint> hints = new ArrayList<>();
        hints.add(
                RelHint.builder(FlinkHints.HINT_NAME_OPTIONS).hintOptions(dynamicOptions).build());
        final ToRelContext toRelContext = ViewExpanders.simpleContext(cluster, hints);
        final RelNode relNode =
                relOptSchema.getTableForMember(identifier.toList()).toRel(toRelContext);
        return push(relNode);
    }

    @Override
    public FlinkTypeFactory getTypeFactory() {
        return (FlinkTypeFactory) super.getTypeFactory();
    }

    @Override
    public RelBuilder transform(UnaryOperator<Config> transform) {
        // Override in order to return a FlinkRelBuilder.
        final Context mergedContext =
                Contexts.of(
                        transform.apply(FLINK_REL_BUILDER_CONFIG),
                        cluster.getPlanner().getContext());
        return FlinkRelBuilder.of(mergedContext, cluster, relOptSchema);
    }

    private static boolean isCountStarAgg(LogicalAggregate agg) {
        if (agg.getGroupCount() != 0 || agg.getAggCallList().size() != 1) {
            return false;
        }
        final AggregateCall call = agg.getAggCallList().get(0);
        return call.getAggregation().getKind() == SqlKind.COUNT
                && call.filterArg == -1
                && call.getArgList().isEmpty();
    }
}
