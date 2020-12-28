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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    private final List<ResolvedExpression> groupingExpressions;
    private final List<ResolvedExpression> aggregateExpressions;
    private final List<ResolvedExpression> windowPropertiesExpressions;
    private final ResolvedGroupWindow groupWindow;
    private final QueryOperation child;
    private final TableSchema tableSchema;

    public WindowAggregateQueryOperation(
            List<ResolvedExpression> groupingExpressions,
            List<ResolvedExpression> aggregateExpressions,
            List<ResolvedExpression> windowPropertiesExpressions,
            ResolvedGroupWindow groupWindow,
            QueryOperation child,
            TableSchema tableSchema) {
        this.groupingExpressions = groupingExpressions;
        this.aggregateExpressions = aggregateExpressions;
        this.windowPropertiesExpressions = windowPropertiesExpressions;
        this.groupWindow = groupWindow;
        this.child = child;
        this.tableSchema = tableSchema;
    }

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
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
    }
}
