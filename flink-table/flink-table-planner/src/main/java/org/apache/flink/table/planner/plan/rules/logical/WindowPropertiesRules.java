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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.ProctimeAttribute;
import org.apache.flink.table.runtime.groupwindow.RowtimeAttribute;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowStart;

import org.apache.flink.shaded.guava32.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;

public class WindowPropertiesRules {

    public static final WindowPropertiesHavingRule WINDOW_PROPERTIES_HAVING_RULE =
            WindowPropertiesHavingRule.WindowPropertiesHavingRuleConfig.DEFAULT.toRule();

    public static final WindowPropertiesRule WINDOW_PROPERTIES_RULE =
            WindowPropertiesRule.WindowPropertiesRuleConfig.DEFAULT.toRule();

    public static class WindowPropertiesRule
            extends RelRule<WindowPropertiesRule.WindowPropertiesRuleConfig> {

        protected WindowPropertiesRule(WindowPropertiesRule.WindowPropertiesRuleConfig config) {
            super(config);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            LogicalProject project = call.rel(0);
            // project includes at least one group auxiliary function
            return project.getProjects().stream()
                    .anyMatch(WindowPropertiesRules::hasGroupAuxiliaries);
        }

        public void onMatch(RelOptRuleCall call) {
            LogicalProject project = call.rel(0);
            LogicalProject innerProject = call.rel(1);
            LogicalWindowAggregate agg = call.rel(2);

            RelNode converted =
                    convertWindowNodes(
                            call.builder(), project, Optional.empty(), innerProject, agg);

            call.transformTo(converted);
        }

        /** Rule configuration. */
        @Value.Immutable(singleton = false)
        public interface WindowPropertiesRuleConfig extends RelRule.Config {
            WindowPropertiesRuleConfig DEFAULT =
                    ImmutableWindowPropertiesRuleConfig.builder()
                            .build()
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(LogicalProject.class)
                                                    .oneInput(
                                                            b1 ->
                                                                    b1.operand(LogicalProject.class)
                                                                            .oneInput(
                                                                                    b2 ->
                                                                                            b2.operand(
                                                                                                            LogicalWindowAggregate
                                                                                                                    .class)
                                                                                                    .noInputs())))
                            .withDescription("WindowPropertiesRule");

            @Override
            default WindowPropertiesRule toRule() {
                return new WindowPropertiesRule(this);
            }
        }
    }

    public static class WindowPropertiesHavingRule
            extends RelRule<WindowPropertiesHavingRule.WindowPropertiesHavingRuleConfig> {

        protected WindowPropertiesHavingRule(WindowPropertiesHavingRuleConfig config) {
            super(config);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            LogicalProject project = call.rel(0);
            LogicalFilter filter = call.rel(1);

            return project.getProjects().stream()
                            .anyMatch(WindowPropertiesRules::hasGroupAuxiliaries)
                    || WindowPropertiesRules.hasGroupAuxiliaries(filter.getCondition());
        }

        public void onMatch(RelOptRuleCall call) {
            LogicalProject project = call.rel(0);
            LogicalFilter filter = call.rel(1);
            LogicalProject innerProject = call.rel(2);
            LogicalWindowAggregate agg = call.rel(3);

            RelNode converted =
                    WindowPropertiesRules.convertWindowNodes(
                            call.builder(), project, Optional.of(filter), innerProject, agg);

            call.transformTo(converted);
        }

        /** Rule configuration. */
        @Value.Immutable(singleton = false)
        public interface WindowPropertiesHavingRuleConfig extends RelRule.Config {
            WindowPropertiesHavingRuleConfig DEFAULT =
                    ImmutableWindowPropertiesHavingRuleConfig.builder()
                            .build()
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(LogicalProject.class)
                                                    .oneInput(
                                                            b1 ->
                                                                    b1.operand(LogicalFilter.class)
                                                                            .oneInput(
                                                                                    b2 ->
                                                                                            b2.operand(
                                                                                                            LogicalProject
                                                                                                                    .class)
                                                                                                    .oneInput(
                                                                                                            b3 ->
                                                                                                                    b3.operand(
                                                                                                                                    LogicalWindowAggregate
                                                                                                                                            .class)
                                                                                                                            .noInputs()))))
                            .withDescription("WindowPropertiesHavingRule");

            @Override
            default WindowPropertiesHavingRule toRule() {
                return new WindowPropertiesHavingRule(this);
            }
        }
    }

    public static RelNode convertWindowNodes(
            RelBuilder builder,
            LogicalProject project,
            Optional<LogicalFilter> filter,
            LogicalProject innerProject,
            LogicalWindowAggregate agg) {
        LogicalWindow w = agg.getWindow();
        WindowType windowType = getWindowType(w);

        NamedWindowProperty startProperty =
                new NamedWindowProperty(
                        propertyName(w, "start"), new WindowStart(w.aliasAttribute()));
        NamedWindowProperty endProperty =
                new NamedWindowProperty(propertyName(w, "end"), new WindowEnd(w.aliasAttribute()));
        List<NamedWindowProperty> startEndProperties = List.of(startProperty, endProperty);

        // allow rowtime/proctime for rowtime windows and proctime for proctime windows
        List<NamedWindowProperty> timeProperties = new ArrayList<>();
        switch (windowType) {
            case STREAM_ROWTIME:
                timeProperties =
                        List.of(
                                new NamedWindowProperty(
                                        propertyName(w, "rowtime"),
                                        new RowtimeAttribute(w.aliasAttribute())),
                                new NamedWindowProperty(
                                        propertyName(w, "proctime"),
                                        new ProctimeAttribute(w.aliasAttribute())));
                break;
            case STREAM_PROCTIME:
                timeProperties =
                        List.of(
                                new NamedWindowProperty(
                                        propertyName(w, "proctime"),
                                        new ProctimeAttribute(w.aliasAttribute())));
                break;
            case BATCH_ROWTIME:
                timeProperties =
                        List.of(
                                new NamedWindowProperty(
                                        propertyName(w, "rowtime"),
                                        new RowtimeAttribute(w.aliasAttribute())));
                break;
            default:
                throw new TableException(
                        "Unknown window type encountered. Please report this bug.");
        }

        List<NamedWindowProperty> properties =
                Lists.newArrayList(Iterables.concat(startEndProperties, timeProperties));

        // retrieve window start and end properties
        builder.push(agg.copy(properties));

        // forward window start and end properties
        List<RexNode> projectNodes =
                Stream.concat(
                                innerProject.getProjects().stream(),
                                properties.stream().map(np -> builder.field(np.getName())))
                        .collect(Collectors.toList());
        builder.project(projectNodes);

        // replace window auxiliary function in filter by access to window properties
        filter.ifPresent(
                f -> builder.filter(replaceGroupAuxiliaries(f.getCondition(), w, builder)));

        // replace window auxiliary unctions in projection by access to window properties
        List<RexNode> finalProjects =
                project.getProjects().stream()
                        .map(expr -> replaceGroupAuxiliaries(expr, w, builder))
                        .collect(Collectors.toList());
        builder.project(finalProjects, project.getRowType().getFieldNames());

        return builder.build();
    }

    private static WindowType getWindowType(LogicalWindow window) {
        if (AggregateUtil.isRowtimeAttribute(window.timeAttribute())) {
            return WindowType.STREAM_ROWTIME;
        } else if (AggregateUtil.isProctimeAttribute(window.timeAttribute())) {
            return WindowType.STREAM_PROCTIME;
        } else if (window.timeAttribute()
                .getOutputDataType()
                .getLogicalType()
                .is(TIMESTAMP_WITHOUT_TIME_ZONE)) {
            return WindowType.BATCH_ROWTIME;
        } else {
            throw new TableException("Unknown window type encountered. Please report this bug.");
        }
    }

    /** Generates a property name for a window. */
    private static String propertyName(LogicalWindow window, String name) {
        return window.aliasAttribute().getName() + name;
    }

    /** Replace group auxiliaries with field references. */
    public static RexNode replaceGroupAuxiliaries(
            RexNode node, LogicalWindow window, RelBuilder builder) {
        RexBuilder rexBuilder = builder.getRexBuilder();
        WindowType windowType = getWindowType(window);

        if (node instanceof RexCall) {
            RexCall c = (RexCall) node;
            if (isWindowStart(c)) {
                return rexBuilder.makeCast(
                        c.getType(), builder.field(propertyName(window, "start")), false);
            } else if (isWindowEnd(c)) {
                return rexBuilder.makeCast(
                        c.getType(), builder.field(propertyName(window, "end")), false);
            } else if (isWindowRowtime(c)) {
                switch (windowType) {
                    case STREAM_ROWTIME:
                    case BATCH_ROWTIME:
                        // replace expression by access to window rowtime
                        return rexBuilder.makeCast(
                                c.getType(), builder.field(propertyName(window, "rowtime")), false);
                    case STREAM_PROCTIME:
                        throw new ValidationException(
                                "A proctime window cannot provide a rowtime attribute.");
                    default:
                        throw new TableException(
                                "Unknown window type encountered. Please report this bug.");
                }

            } else if (isWindowProctime(c)) {
                switch (windowType) {
                    case STREAM_PROCTIME:
                    case STREAM_ROWTIME:
                        // replace expression by access to window proctime
                        return rexBuilder.makeCast(
                                c.getType(),
                                builder.field(propertyName(window, "proctime")),
                                false);
                    case BATCH_ROWTIME:
                        throw new ValidationException(
                                "PROCTIME window property is not supported in batch queries.");
                    default:
                        throw new TableException(
                                "Unknown window type encountered. Please report this bug.");
                }

            } else {
                List<RexNode> newOps =
                        c.getOperands().stream()
                                .map(op -> replaceGroupAuxiliaries(op, window, builder))
                                .collect(Collectors.toList());
                return c.clone(c.getType(), newOps);
            }
        } else {
            // preserve expression
            return node;
        }
    }

    /** Checks if a RexNode is a window start auxiliary function. */
    private static boolean isWindowStart(RexNode node) {
        if (node instanceof RexCall) {
            RexCall c = (RexCall) node;
            if (c.getOperator().isGroupAuxiliary()) {
                return c.getOperator() == FlinkSqlOperatorTable.TUMBLE_START
                        || c.getOperator() == FlinkSqlOperatorTable.HOP_START
                        || c.getOperator() == FlinkSqlOperatorTable.SESSION_START;
            }
        }
        return false;
    }

    /** Checks if a RexNode is a window end auxiliary function. */
    private static boolean isWindowEnd(RexNode node) {
        if (node instanceof RexCall) {
            RexCall c = (RexCall) node;
            if (c.getOperator().isGroupAuxiliary()) {
                return c.getOperator() == FlinkSqlOperatorTable.TUMBLE_END
                        || c.getOperator() == FlinkSqlOperatorTable.HOP_END
                        || c.getOperator() == FlinkSqlOperatorTable.SESSION_END;
            }
        }
        return false;
    }

    /** Checks if a RexNode is a window rowtime auxiliary function. */
    private static boolean isWindowRowtime(RexNode node) {
        if (node instanceof RexCall) {
            RexCall c = (RexCall) node;
            if (c.getOperator().isGroupAuxiliary()) {
                return c.getOperator() == FlinkSqlOperatorTable.TUMBLE_ROWTIME
                        || c.getOperator() == FlinkSqlOperatorTable.HOP_ROWTIME
                        || c.getOperator() == FlinkSqlOperatorTable.SESSION_ROWTIME;
            }
        }
        return false;
    }

    /** Checks if a RexNode is a window proctime auxiliary function. */
    private static boolean isWindowProctime(RexNode node) {
        if (node instanceof RexCall) {
            RexCall c = (RexCall) node;
            if (c.getOperator().isGroupAuxiliary()) {
                return c.getOperator() == FlinkSqlOperatorTable.TUMBLE_PROCTIME
                        || c.getOperator() == FlinkSqlOperatorTable.HOP_PROCTIME
                        || c.getOperator() == FlinkSqlOperatorTable.SESSION_PROCTIME;
            }
        }
        return false;
    }

    public static boolean hasGroupAuxiliaries(RexNode node) {
        if (node instanceof RexCall) {
            RexCall c = (RexCall) node;
            if (c.getOperator().isGroupAuxiliary()) {
                return true;
            }
            return c.getOperands().stream().anyMatch(WindowPropertiesRules::hasGroupAuxiliaries);
        }
        return false;
    }

    public static boolean hasGroupFunction(RexNode node) {
        if (node instanceof RexCall) {
            RexCall c = (RexCall) node;
            if (c.getOperator().isGroup()) {
                return true;
            }
            return c.getOperands().stream().anyMatch(WindowPropertiesRules::hasGroupFunction);
        }
        return false;
    }

    enum WindowType {
        STREAM_ROWTIME,
        STREAM_PROCTIME,
        BATCH_ROWTIME
    }
}
