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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.SqlFactory;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.operations.utils.OperationExpressionsUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.operations.WindowAggregateQueryOperation.ResolvedGroupWindow.WindowType.SESSION;
import static org.apache.flink.table.operations.WindowAggregateQueryOperation.ResolvedGroupWindow.WindowType.SLIDE;
import static org.apache.flink.table.operations.WindowAggregateQueryOperation.ResolvedGroupWindow.WindowType.TUMBLE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Relational operation that performs computations on top of subsets of input rows grouped by key
 * and group window. It differs from {@link AggregateQueryOperation} by the group window.
 */
@Internal
public class WindowAggregateQueryOperation implements QueryOperation {

    private static final String INPUT_ALIAS = "$$T_WIN_AGG";

    // Output columns of a windowing TVF, which is what window properties become in SQL.
    private static final String WINDOW_START_COLUMN =
            BuiltInFunctionDefinitions.WINDOW_START.getSqlName();
    private static final String WINDOW_END_COLUMN =
            BuiltInFunctionDefinitions.WINDOW_END.getSqlName();
    private static final String WINDOW_TIME_COLUMN = "window_time";

    private final List<ResolvedExpression> groupingExpressions;
    private final List<ResolvedExpression> aggregateExpressions;
    private final List<ResolvedExpression> windowPropertiesExpressions;
    private final ResolvedGroupWindow groupWindow;
    private final QueryOperation child;
    private final ResolvedSchema resolvedSchema;

    public WindowAggregateQueryOperation(
            List<ResolvedExpression> groupingExpressions,
            List<ResolvedExpression> aggregateExpressions,
            List<ResolvedExpression> windowPropertiesExpressions,
            ResolvedGroupWindow groupWindow,
            QueryOperation child,
            ResolvedSchema resolvedSchema) {
        this.groupingExpressions = groupingExpressions;
        this.aggregateExpressions = aggregateExpressions;
        this.windowPropertiesExpressions = windowPropertiesExpressions;
        this.groupWindow = groupWindow;
        this.child = child;
        this.resolvedSchema = resolvedSchema;
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("group", groupingExpressions);
        args.put("agg", aggregateExpressions);
        args.put("windowProperties", windowPropertiesExpressions);
        args.put("window", groupWindow.asSummaryString());

        return OperationUtils.formatWithChildren(
                "WindowAggregate", args, getChildren(), Operation::asSummaryString);
    }

    @Override
    public String asSerializableString(SqlFactory sqlFactory) {
        final List<WindowColumn> windowColumns = resolveWindowColumns();
        return String.format(
                "SELECT %s FROM TABLE(%s\n) %s GROUP BY %s",
                serializeSelectList(windowColumns, sqlFactory),
                OperationUtils.indent(
                        groupWindow.asSerializableString(
                                child.asSerializableString(sqlFactory), sqlFactory)),
                INPUT_ALIAS,
                serializeGroupBy(windowColumns, sqlFactory));
    }

    private List<WindowColumn> resolveWindowColumns() {
        return windowPropertiesExpressions.stream()
                .map(property -> new WindowColumn(aliasOf(property), windowColumnOf(property)))
                .collect(Collectors.toList());
    }

    private static String aliasOf(ResolvedExpression aliasedProperty) {
        return OperationExpressionsUtils.extractName(aliasedProperty)
                .orElseThrow(
                        () ->
                                new TableException(
                                        "Expected a named alias over a window property. Got: "
                                                + aliasedProperty));
    }

    /** The windowing TVF output column that the given window property denotes. */
    private String windowColumnOf(ResolvedExpression aliasedProperty) {
        final FunctionDefinition property = windowPropertyOf(aliasedProperty);
        if (BuiltInFunctionDefinitions.WINDOW_START == property) {
            return WINDOW_START_COLUMN;
        }
        if (BuiltInFunctionDefinitions.WINDOW_END == property) {
            return WINDOW_END_COLUMN;
        }
        if (BuiltInFunctionDefinitions.ROWTIME == property) {
            return WINDOW_TIME_COLUMN;
        }
        if (BuiltInFunctionDefinitions.PROCTIME == property) {
            checkWindowIsProcessingTime();
            return WINDOW_TIME_COLUMN;
        }
        throw new TableException("Unsupported window property: " + property);
    }

    private static FunctionDefinition windowPropertyOf(ResolvedExpression aliasedProperty) {
        final List<ResolvedExpression> children = aliasedProperty.getResolvedChildren();
        if (!children.isEmpty() && children.get(0) instanceof CallExpression) {
            final FunctionDefinition property =
                    ((CallExpression) children.get(0)).getFunctionDefinition();
            if (OperationExpressionsUtils.WINDOW_PROPERTIES.contains(property)) {
                return property;
            }
        }
        throw new TableException(
                "Expected an aliased window property call. Got: " + aliasedProperty);
    }

    /**
     * Rejects a processing-time property on an event-time group window.
     *
     * <p>The {@code window_time} column of a windowing TVF derives its time attribute kind from the
     * window, not from the requested property, so it cannot express a processing-time attribute of
     * an event-time window. Since a {@code rowtime} property on a processing-time window is
     * rejected during expression resolution (see {@code WindowTimeIndictorInputTypeStrategy}), we
     * reject the other case here rather than serializing it.
     */
    private void checkWindowIsProcessingTime() {
        final LogicalType windowTimeAttribute =
                groupWindow.getTimeAttribute().getOutputDataType().getLogicalType();
        if (LogicalTypeChecks.isProctimeAttribute(windowTimeAttribute)) {
            return;
        }
        throw new TableException(
                String.format(
                        "The processing-time property of the event-time group window '%s' "
                                + "cannot be expressed in windowing-TVF syntax. The window_time "
                                + "column of a windowing TVF always has the time attribute kind "
                                + "of the window itself, so it cannot represent a wall-clock "
                                + "processing-time attribute. Define the group window on a "
                                + "processing-time attribute instead.",
                        groupWindow.getAlias()));
    }

    private String serializeSelectList(List<WindowColumn> windowColumns, SqlFactory sqlFactory) {
        return Stream.concat(
                        Stream.concat(groupingExpressions.stream(), aggregateExpressions.stream())
                                .map(expr -> scopedToInput(expr, sqlFactory)),
                        windowColumns.stream().map(WindowColumn::asSelectItem))
                .collect(Collectors.joining(", "));
    }

    private String scopedToInput(ResolvedExpression expr, SqlFactory sqlFactory) {
        return OperationExpressionsUtils.scopeReferencesWithAlias(INPUT_ALIAS, expr)
                .asSerializableString(sqlFactory);
    }

    private String serializeGroupBy(List<WindowColumn> windowColumns, SqlFactory sqlFactory) {
        final Stream<String> groupedWindowColumns =
                windowColumns.stream().anyMatch(WindowColumn::isWindowTime)
                        ? Stream.of(WINDOW_START_COLUMN, WINDOW_END_COLUMN, WINDOW_TIME_COLUMN)
                        : Stream.of(WINDOW_START_COLUMN, WINDOW_END_COLUMN);
        return Stream.concat(
                        groupedWindowColumns,
                        groupingExpressions.stream().map(expr -> scopedToInput(expr, sqlFactory)))
                .collect(Collectors.joining(", "));
    }

    public List<ResolvedExpression> getGroupingExpressions() {
        return groupingExpressions;
    }

    public List<ResolvedExpression> getAggregateExpressions() {
        return aggregateExpressions;
    }

    public List<ResolvedExpression> getWindowPropertiesExpressions() {
        return windowPropertiesExpressions;
    }

    public ResolvedGroupWindow getGroupWindow() {
        return groupWindow;
    }

    @Override
    public List<QueryOperation> getChildren() {
        return Collections.singletonList(child);
    }

    @Override
    public <T> T accept(QueryOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** A windowing TVF output column, projected under the alias of a window property. */
    private static final class WindowColumn {

        private final String alias;
        private final String column;

        private WindowColumn(String alias, String column) {
            this.alias = alias;
            this.column = column;
        }

        private boolean isWindowTime() {
            return WINDOW_TIME_COLUMN.equals(column);
        }

        private String asSelectItem() {
            return String.format("(%s) AS %s", column, EncodingUtils.escapeIdentifier(alias));
        }
    }

    /** Wrapper for resolved expressions of a {@link org.apache.flink.table.api.GroupWindow}. */
    @Internal
    public static class ResolvedGroupWindow {

        private final WindowType type;
        private final String alias;
        private final FieldReferenceExpression timeAttribute;
        private final ValueLiteralExpression slide;
        private final ValueLiteralExpression size;
        private final ValueLiteralExpression gap;

        /** The type of window. */
        @Internal
        public enum WindowType {
            SLIDE,
            SESSION,
            TUMBLE
        }

        /** Size, slide and gap can be null depending on the window type. */
        private ResolvedGroupWindow(
                WindowType type,
                String alias,
                FieldReferenceExpression timeAttribute,
                @Nullable ValueLiteralExpression size,
                @Nullable ValueLiteralExpression slide,
                @Nullable ValueLiteralExpression gap) {
            checkArgument(!StringUtils.isNullOrWhitespaceOnly(alias));
            this.type = type;
            this.timeAttribute = checkNotNull(timeAttribute);
            this.alias = alias;
            this.slide = slide;
            this.size = size;
            this.gap = gap;
        }

        public static ResolvedGroupWindow slidingWindow(
                String alias,
                FieldReferenceExpression timeAttribute,
                ValueLiteralExpression size,
                ValueLiteralExpression slide) {
            checkNotNull(size);
            checkNotNull(slide);
            return new ResolvedGroupWindow(SLIDE, alias, timeAttribute, size, slide, null);
        }

        public static ResolvedGroupWindow tumblingWindow(
                String alias, FieldReferenceExpression timeAttribute, ValueLiteralExpression size) {
            checkNotNull(size);
            return new ResolvedGroupWindow(TUMBLE, alias, timeAttribute, size, null, null);
        }

        public static ResolvedGroupWindow sessionWindow(
                String alias, FieldReferenceExpression timeAttribute, ValueLiteralExpression gap) {
            checkNotNull(gap);
            return new ResolvedGroupWindow(SESSION, alias, timeAttribute, null, null, gap);
        }

        public WindowType getType() {
            return type;
        }

        public FieldReferenceExpression getTimeAttribute() {
            return timeAttribute;
        }

        public String getAlias() {
            return alias;
        }

        /**
         * Slide of {@link WindowType#SLIDE} window. Empty for other windows.
         *
         * @return slide of a slide window
         */
        public Optional<ValueLiteralExpression> getSlide() {
            return Optional.of(slide);
        }

        /**
         * Size of a {@link WindowType#TUMBLE} or {@link WindowType#SLIDE} window. Empty for {@link
         * WindowType#SESSION} window.
         *
         * @return size of a window
         */
        public Optional<ValueLiteralExpression> getSize() {
            return Optional.of(size);
        }

        /**
         * Gap of a {@link WindowType#SESSION} window. Empty for other types of windows.
         *
         * @return gap of a session window
         */
        public Optional<ValueLiteralExpression> getGap() {
            return Optional.of(gap);
        }

        public String asSummaryString() {
            switch (type) {
                case SLIDE:
                    return String.format(
                            "SlideWindow(field: [%s], slide: [%s], size: [%s])",
                            timeAttribute, slide, size);
                case SESSION:
                    return String.format(
                            "SessionWindow(field: [%s], gap: [%s])", timeAttribute, gap);
                case TUMBLE:
                    return String.format(
                            "TumbleWindow(field: [%s], size: [%s])", timeAttribute, size);
                default:
                    throw new IllegalStateException("Unknown window type: " + type);
            }
        }

        public String asSerializableString(String table, SqlFactory sqlFactory) {
            switch (type) {
                case SLIDE:
                    return String.format(
                            "HOP((%s\n), DESCRIPTOR(%s), %s, %s)",
                            OperationUtils.indent(table),
                            timeAttribute.asSerializableString(sqlFactory),
                            slide.asSerializableString(sqlFactory),
                            size.asSerializableString(sqlFactory));
                case SESSION:
                    throw new TableException("Session windows are not SQL serializable yet.");
                case TUMBLE:
                    return String.format(
                            "TUMBLE((%s\n), DESCRIPTOR(%s), %s)",
                            OperationUtils.indent(table),
                            timeAttribute.asSerializableString(sqlFactory),
                            size.asSerializableString(sqlFactory));
                default:
                    throw new IllegalStateException("Unknown window type: " + type);
            }
        }
    }
}
