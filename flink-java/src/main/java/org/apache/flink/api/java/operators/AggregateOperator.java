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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.aggregation.AggregationFunctionFactory;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * This operator represents the application of a "aggregate" operation on a data set, and the result
 * data set produced by the function.
 *
 * @param <IN> The type of the data set aggregated by the operator.
 */
@Public
public class AggregateOperator<IN> extends SingleInputOperator<IN, IN, AggregateOperator<IN>> {

    private final List<AggregationFunction<?>> aggregationFunctions = new ArrayList<>(4);

    private final List<Integer> fields = new ArrayList<>(4);

    private final Grouping<IN> grouping;

    private final String aggregateLocationName;

    /** Non grouped aggregation. */
    public AggregateOperator(
            DataSet<IN> input, Aggregations function, int field, String aggregateLocationName) {
        super(Preconditions.checkNotNull(input), input.getType());
        Preconditions.checkNotNull(function);

        this.aggregateLocationName = aggregateLocationName;

        if (!input.getType().isTupleType()) {
            throw new InvalidProgramException(
                    "Aggregating on field positions is only possible on tuple data types.");
        }

        TupleTypeInfoBase<?> inType = (TupleTypeInfoBase<?>) input.getType();

        if (field < 0 || field >= inType.getArity()) {
            throw new IllegalArgumentException("Aggregation field position is out of range.");
        }

        AggregationFunctionFactory factory = function.getFactory();
        AggregationFunction<?> aggFunct =
                factory.createAggregationFunction(inType.getTypeAt(field).getTypeClass());

        // this is the first aggregation operator after a regular data set (non grouped aggregation)
        this.aggregationFunctions.add(aggFunct);
        this.fields.add(field);
        this.grouping = null;
    }

    /**
     * Grouped aggregation.
     *
     * @param input
     * @param function
     * @param field
     */
    public AggregateOperator(
            Grouping<IN> input, Aggregations function, int field, String aggregateLocationName) {
        super(
                Preconditions.checkNotNull(input).getInputDataSet(),
                input.getInputDataSet().getType());
        Preconditions.checkNotNull(function);

        this.aggregateLocationName = aggregateLocationName;

        if (!input.getInputDataSet().getType().isTupleType()) {
            throw new InvalidProgramException(
                    "Aggregating on field positions is only possible on tuple data types.");
        }

        TupleTypeInfoBase<?> inType = (TupleTypeInfoBase<?>) input.getInputDataSet().getType();

        if (field < 0 || field >= inType.getArity()) {
            throw new IllegalArgumentException("Aggregation field position is out of range.");
        }

        AggregationFunctionFactory factory = function.getFactory();
        AggregationFunction<?> aggFunct =
                factory.createAggregationFunction(inType.getTypeAt(field).getTypeClass());

        // set the aggregation fields
        this.aggregationFunctions.add(aggFunct);
        this.fields.add(field);
        this.grouping = input;
    }

    public AggregateOperator<IN> and(Aggregations function, int field) {
        Preconditions.checkNotNull(function);

        TupleTypeInfoBase<?> inType = (TupleTypeInfoBase<?>) getType();

        if (field < 0 || field >= inType.getArity()) {
            throw new IllegalArgumentException("Aggregation field position is out of range.");
        }

        AggregationFunctionFactory factory = function.getFactory();
        AggregationFunction<?> aggFunct =
                factory.createAggregationFunction(inType.getTypeAt(field).getTypeClass());

        this.aggregationFunctions.add(aggFunct);
        this.fields.add(field);

        return this;
    }

    public AggregateOperator<IN> andSum(int field) {
        return this.and(Aggregations.SUM, field);
    }

    public AggregateOperator<IN> andMin(int field) {
        return this.and(Aggregations.MIN, field);
    }

    public AggregateOperator<IN> andMax(int field) {
        return this.and(Aggregations.MAX, field);
    }

    @SuppressWarnings("unchecked")
    @Override
    @Internal
    protected org.apache.flink.api.common.operators.base.GroupReduceOperatorBase<
                    IN, IN, GroupReduceFunction<IN, IN>>
            translateToDataFlow(Operator<IN> input) {

        // sanity check
        if (this.aggregationFunctions.isEmpty()
                || this.aggregationFunctions.size() != this.fields.size()) {
            throw new IllegalStateException();
        }

        // construct the aggregation function
        AggregationFunction<Object>[] aggFunctions =
                new AggregationFunction[this.aggregationFunctions.size()];
        int[] fields = new int[this.fields.size()];
        StringBuilder genName = new StringBuilder();

        for (int i = 0; i < fields.length; i++) {
            aggFunctions[i] = (AggregationFunction<Object>) this.aggregationFunctions.get(i);
            fields[i] = this.fields.get(i);

            genName.append(aggFunctions[i].toString())
                    .append('(')
                    .append(fields[i])
                    .append(')')
                    .append(',');
        }
        genName.append(" at ").append(aggregateLocationName);
        genName.setLength(genName.length() - 1);

        @SuppressWarnings("rawtypes")
        RichGroupReduceFunction<IN, IN> function = new AggregatingUdf(aggFunctions, fields);

        String name = getName() != null ? getName() : genName.toString();

        // distinguish between grouped reduce and non-grouped reduce
        if (this.grouping == null) {
            // non grouped aggregation
            UnaryOperatorInformation<IN, IN> operatorInfo =
                    new UnaryOperatorInformation<>(getInputType(), getResultType());
            GroupReduceOperatorBase<IN, IN, GroupReduceFunction<IN, IN>> po =
                    new GroupReduceOperatorBase<IN, IN, GroupReduceFunction<IN, IN>>(
                            function, operatorInfo, new int[0], name);

            po.setCombinable(true);

            // set input
            po.setInput(input);
            // set parallelism
            po.setParallelism(this.getParallelism());

            return po;
        }

        if (this.grouping.getKeys() instanceof Keys.ExpressionKeys) {
            // grouped aggregation
            int[] logicalKeyPositions = this.grouping.getKeys().computeLogicalKeyPositions();
            UnaryOperatorInformation<IN, IN> operatorInfo =
                    new UnaryOperatorInformation<>(getInputType(), getResultType());
            GroupReduceOperatorBase<IN, IN, GroupReduceFunction<IN, IN>> po =
                    new GroupReduceOperatorBase<IN, IN, GroupReduceFunction<IN, IN>>(
                            function, operatorInfo, logicalKeyPositions, name);

            po.setCombinable(true);

            po.setInput(input);
            po.setParallelism(this.getParallelism());
            po.setCustomPartitioner(grouping.getCustomPartitioner());

            SingleInputSemanticProperties props = new SingleInputSemanticProperties();

            for (int keyField : logicalKeyPositions) {
                boolean keyFieldUsedInAgg = false;

                for (int aggField : fields) {
                    if (keyField == aggField) {
                        keyFieldUsedInAgg = true;
                        break;
                    }
                }

                if (!keyFieldUsedInAgg) {
                    props.addForwardedField(keyField, keyField);
                }
            }

            po.setSemanticProperties(props);

            return po;
        } else if (this.grouping.getKeys() instanceof Keys.SelectorFunctionKeys) {
            throw new UnsupportedOperationException(
                    "Aggregate does not support grouping with KeySelector functions, yet.");
        } else {
            throw new UnsupportedOperationException("Unrecognized key type.");
        }
    }

    // --------------------------------------------------------------------------------------------

    @Internal
    private static final class AggregatingUdf<T extends Tuple> extends RichGroupReduceFunction<T, T>
            implements GroupCombineFunction<T, T> {

        private static final long serialVersionUID = 1L;

        private final int[] fieldPositions;

        private final AggregationFunction<Object>[] aggFunctions;

        public AggregatingUdf(AggregationFunction<Object>[] aggFunctions, int[] fieldPositions) {
            Preconditions.checkNotNull(aggFunctions);
            Preconditions.checkNotNull(aggFunctions);
            Preconditions.checkArgument(aggFunctions.length == fieldPositions.length);

            this.aggFunctions = aggFunctions;
            this.fieldPositions = fieldPositions;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            for (AggregationFunction<Object> aggFunction : aggFunctions) {
                aggFunction.initializeAggregate();
            }
        }

        @Override
        public void reduce(Iterable<T> records, Collector<T> out) {
            final AggregationFunction<Object>[] aggFunctions = this.aggFunctions;
            final int[] fieldPositions = this.fieldPositions;

            // aggregators are initialized from before

            T outT = null;
            for (T record : records) {
                outT = record;

                for (int i = 0; i < fieldPositions.length; i++) {
                    Object val = record.getFieldNotNull(fieldPositions[i]);
                    aggFunctions[i].aggregate(val);
                }
            }

            for (int i = 0; i < fieldPositions.length; i++) {
                Object aggVal = aggFunctions[i].getAggregate();
                outT.setField(aggVal, fieldPositions[i]);
                aggFunctions[i].initializeAggregate();
            }

            out.collect(outT);
        }

        @Override
        public void combine(Iterable<T> records, Collector<T> out) {
            reduce(records, out);
        }
    }
}
