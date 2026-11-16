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

package org.apache.flink.table.runtime.functions.ptf;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.ExpressionEvaluator;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategyUtils.Keep;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.invoke.MethodHandle;
import java.time.Duration;
import java.util.Optional;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.ifThenElse;
import static org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategy.ARG_INPUT;
import static org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategy.ARG_STATE_TTL;

/** Record ordering of the built-in deduplication PTFs. */
@Internal
public final class DeduplicateUtils {

    public static TableSemantics tableSemantics(final SpecializedContext context) {
        return context.getCallContext()
                .getTableSemantics(ARG_INPUT)
                .orElseThrow(() -> new IllegalStateException("Table argument expected."));
    }

    /** Returns a value state of the given type that expires after the {@code state_ttl}. */
    public static StateTypeStrategy stateStrategy(
            final SpecializedContext context, final DataType valueType) {
        return StateTypeStrategy.of(
                callContext -> Optional.of(ValueView.newValueViewDataType(valueType)),
                context.getCallContext()
                        .getArgumentValue(ARG_STATE_TTL, Duration.class)
                        .orElse(null));
    }

    /**
     * Returns whether a new record replaces the current one, given the order of the new record
     * relative to the current one: with {@code last} the later arrival wins on a tie, with {@code
     * first} only a strictly smaller record replaces.
     */
    public static boolean replaces(final Keep keep, final int order) {
        return keep == Keep.LAST ? order >= 0 : order < 0;
    }

    /**
     * Compares two records by their {@code sequence_by} values in ascending order. The first column
     * decides, later ones break ties, and NULL orders below every other value.
     */
    @Internal
    public static final class SequenceComparator implements Serializable {

        private static final long serialVersionUID = 1L;

        private final int[] leftIndices;

        private final int[] rightIndices;

        private final DataType[] types;

        private final ExpressionEvaluator[] evaluators;

        private transient MethodHandle[] compareHandles;

        private transient RowData.FieldGetter[] leftGetters;

        private transient RowData.FieldGetter[] rightGetters;

        /**
         * Creates a comparator of the columns at {@code leftIndices} of the left record against the
         * columns at {@code rightIndices} of the right record, both of the given types.
         */
        public SequenceComparator(
                final SpecializedContext context,
                final int[] leftIndices,
                final int[] rightIndices,
                final DataType[] types) {
            this.leftIndices = leftIndices;
            this.rightIndices = rightIndices;
            this.types = types;
            this.evaluators = new ExpressionEvaluator[types.length];
            for (int i = 0; i < types.length; i++) {
                final DataType type = types[i].notNull().toInternal();
                evaluators[i] =
                        context.createEvaluator(
                                ifThenElse(
                                        $("a").isLess($("b")),
                                        -1,
                                        ifThenElse($("a").isGreater($("b")), 1, 0)),
                                DataTypes.INT().notNull(),
                                DataTypes.FIELD("a", type),
                                DataTypes.FIELD("b", type));
            }
        }

        public void open(final FunctionContext context) throws Exception {
            compareHandles = new MethodHandle[types.length];
            leftGetters = new RowData.FieldGetter[types.length];
            rightGetters = new RowData.FieldGetter[types.length];
            for (int i = 0; i < types.length; i++) {
                compareHandles[i] = evaluators[i].open(context);
                leftGetters[i] =
                        RowData.createFieldGetter(types[i].getLogicalType(), leftIndices[i]);
                rightGetters[i] =
                        RowData.createFieldGetter(types[i].getLogicalType(), rightIndices[i]);
            }
        }

        public void close() throws Exception {
            for (final ExpressionEvaluator evaluator : evaluators) {
                evaluator.close();
            }
        }

        /**
         * Returns a negative number, zero, or a positive number if the left record orders before,
         * equal to, or after the right one.
         */
        public int compare(final RowData left, final RowData right) {
            for (int i = 0; i < types.length; i++) {
                final int order =
                        compare(
                                i,
                                leftGetters[i].getFieldOrNull(left),
                                rightGetters[i].getFieldOrNull(right));
                if (order != 0) {
                    return order;
                }
            }
            return 0;
        }

        private int compare(
                final int column, @Nullable final Object left, @Nullable final Object right) {
            if (left == null || right == null) {
                return left == null ? (right == null ? 0 : -1) : 1;
            }
            try {
                return (int) compareHandles[column].invoke(left, right);
            } catch (Throwable t) {
                throw new FlinkRuntimeException(t);
            }
        }
    }

    private DeduplicateUtils() {}
}
