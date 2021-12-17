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
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Keys.SelectorFunctionKeys;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.translation.PlanUnwrappingReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * This operator represents the application of a "distinct" function on a data set, and the result
 * data set produced by the function.
 *
 * @param <T> The type of the data set made distinct by the operator.
 */
@Public
public class DistinctOperator<T> extends SingleInputOperator<T, T, DistinctOperator<T>> {

    private final Keys<T> keys;

    private final String distinctLocationName;

    private CombineHint hint = CombineHint.OPTIMIZER_CHOOSES;

    public DistinctOperator(DataSet<T> input, Keys<T> keys, String distinctLocationName) {
        super(input, input.getType());

        this.distinctLocationName = distinctLocationName;

        // if keys is null distinction is done on all fields
        if (keys == null) {
            keys = new Keys.ExpressionKeys<>(input.getType());
        }

        this.keys = keys;
    }

    @Override
    protected org.apache.flink.api.common.operators.SingleInputOperator<?, T, ?>
            translateToDataFlow(Operator<T> input) {

        final ReduceFunction<T> function = new DistinctFunction<>();

        String name = getName() != null ? getName() : "Distinct at " + distinctLocationName;

        if (keys instanceof Keys.ExpressionKeys) {

            int[] logicalKeyPositions = keys.computeLogicalKeyPositions();
            UnaryOperatorInformation<T, T> operatorInfo =
                    new UnaryOperatorInformation<>(getInputType(), getResultType());
            ReduceOperatorBase<T, ReduceFunction<T>> po =
                    new ReduceOperatorBase<>(function, operatorInfo, logicalKeyPositions, name);

            po.setCombineHint(hint);
            po.setInput(input);
            po.setParallelism(getParallelism());

            // make sure that distinct preserves the partitioning for the fields on which they
            // operate
            if (getType().isTupleType()) {
                SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();

                for (int field : keys.computeLogicalKeyPositions()) {
                    sProps.addForwardedField(field, field);
                }

                po.setSemanticProperties(sProps);
            }

            return po;
        } else if (keys instanceof SelectorFunctionKeys) {

            @SuppressWarnings("unchecked")
            SelectorFunctionKeys<T, ?> selectorKeys = (SelectorFunctionKeys<T, ?>) keys;

            org.apache.flink.api.common.operators.SingleInputOperator<?, T, ?> po =
                    translateSelectorFunctionDistinct(
                            selectorKeys,
                            function,
                            getResultType(),
                            name,
                            input,
                            parallelism,
                            hint);

            return po;
        } else {
            throw new UnsupportedOperationException("Unrecognized key type.");
        }
    }

    /**
     * Sets the strategy to use for the combine phase of the reduce.
     *
     * <p>If this method is not called, then the default hint will be used. ({@link
     * org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint#OPTIMIZER_CHOOSES})
     *
     * @param strategy The hint to use.
     * @return The DistinctOperator object, for function call chaining.
     */
    @PublicEvolving
    public DistinctOperator<T> setCombineHint(CombineHint strategy) {
        this.hint = strategy;
        return this;
    }

    // --------------------------------------------------------------------------------------------

    private static <IN, K>
            org.apache.flink.api.common.operators.SingleInputOperator<?, IN, ?>
                    translateSelectorFunctionDistinct(
                            SelectorFunctionKeys<IN, ?> rawKeys,
                            ReduceFunction<IN> function,
                            TypeInformation<IN> outputType,
                            String name,
                            Operator<IN> input,
                            int parallelism,
                            CombineHint hint) {
        @SuppressWarnings("unchecked")
        final SelectorFunctionKeys<IN, K> keys = (SelectorFunctionKeys<IN, K>) rawKeys;

        TypeInformation<Tuple2<K, IN>> typeInfoWithKey = KeyFunctions.createTypeWithKey(keys);
        Operator<Tuple2<K, IN>> keyedInput = KeyFunctions.appendKeyExtractor(input, keys);

        PlanUnwrappingReduceOperator<IN, K> reducer =
                new PlanUnwrappingReduceOperator<>(
                        function, keys, name, outputType, typeInfoWithKey);
        reducer.setInput(keyedInput);
        reducer.setCombineHint(hint);
        reducer.setParallelism(parallelism);

        return KeyFunctions.appendKeyRemover(reducer, keys);
    }

    @Internal
    private static final class DistinctFunction<T> implements ReduceFunction<T> {

        private static final long serialVersionUID = 1L;

        @Override
        public T reduce(T value1, T value2) throws Exception {
            return value1;
        }
    }
}
