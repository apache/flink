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
import org.apache.flink.api.common.functions.CombineFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Keys.ExpressionKeys;
import org.apache.flink.api.common.operators.Keys.SelectorFunctionKeys;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.operators.translation.CombineToGroupCombineWrapper;
import org.apache.flink.api.java.operators.translation.PlanUnwrappingReduceGroupOperator;
import org.apache.flink.api.java.operators.translation.PlanUnwrappingSortedReduceGroupOperator;
import org.apache.flink.api.java.operators.translation.RichCombineToGroupCombineWrapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * This operator represents the application of a "reduceGroup" function on a data set, and the
 * result data set produced by the function.
 *
 * @param <IN> The type of the data set consumed by the operator.
 * @param <OUT> The type of the data set created by the operator.
 */
@Public
public class GroupReduceOperator<IN, OUT>
        extends SingleInputUdfOperator<IN, OUT, GroupReduceOperator<IN, OUT>> {

    private static final Logger LOG = LoggerFactory.getLogger(GroupReduceOperator.class);

    private GroupReduceFunction<IN, OUT> function;

    private final Grouping<IN> grouper;

    private final String defaultName;

    private boolean combinable;

    /**
     * Constructor for a non-grouped reduce (all reduce).
     *
     * @param input The input data set to the groupReduce function.
     * @param function The user-defined GroupReduce function.
     */
    public GroupReduceOperator(
            DataSet<IN> input,
            TypeInformation<OUT> resultType,
            GroupReduceFunction<IN, OUT> function,
            String defaultName) {
        super(input, resultType);

        this.function = function;
        this.grouper = null;
        this.defaultName = defaultName;

        this.combinable = checkCombinability();
    }

    /**
     * Constructor for a grouped reduce.
     *
     * @param input The grouped input to be processed group-wise by the groupReduce function.
     * @param function The user-defined GroupReduce function.
     */
    public GroupReduceOperator(
            Grouping<IN> input,
            TypeInformation<OUT> resultType,
            GroupReduceFunction<IN, OUT> function,
            String defaultName) {
        super(input != null ? input.getInputDataSet() : null, resultType);

        this.function = function;
        this.grouper = input;
        this.defaultName = defaultName;

        this.combinable = checkCombinability();
    }

    private boolean checkCombinability() {
        if (function instanceof GroupCombineFunction || function instanceof CombineFunction) {

            // check if the generic types of GroupCombineFunction and GroupReduceFunction match,
            // i.e.,
            //   GroupCombineFunction<IN, IN> and GroupReduceFunction<IN, OUT>.
            // This is a best effort check. If the check cannot be done, we might fail at runtime.
            Type[] reduceTypes = null;
            Type[] combineTypes = null;

            Type[] genInterfaces = function.getClass().getGenericInterfaces();
            for (Type genInterface : genInterfaces) {
                if (genInterface instanceof ParameterizedType) {
                    // get parameters of GroupReduceFunction
                    if (((ParameterizedType) genInterface)
                            .getRawType()
                            .equals(GroupReduceFunction.class)) {
                        reduceTypes = ((ParameterizedType) genInterface).getActualTypeArguments();
                        // get parameters of GroupCombineFunction
                    } else if ((((ParameterizedType) genInterface)
                                    .getRawType()
                                    .equals(GroupCombineFunction.class))
                            || (((ParameterizedType) genInterface)
                                    .getRawType()
                                    .equals(CombineFunction.class))) {

                        combineTypes = ((ParameterizedType) genInterface).getActualTypeArguments();
                    }
                }
            }

            if (reduceTypes != null
                    && reduceTypes.length == 2
                    && combineTypes != null
                    && combineTypes.length == 2) {

                if (reduceTypes[0].equals(combineTypes[0])
                        && reduceTypes[0].equals(combineTypes[1])) {
                    return true;
                } else {
                    LOG.warn(
                            "GroupCombineFunction cannot be used as combiner for GroupReduceFunction. "
                                    + "Generic types are incompatible.");
                    return false;
                }
            } else if (reduceTypes == null || reduceTypes.length != 2) {
                LOG.warn(
                        "Cannot check generic types of GroupReduceFunction. "
                                + "Enabling combiner but combine function might fail at runtime.");
                return true;
            } else {
                LOG.warn(
                        "Cannot check generic types of GroupCombineFunction. "
                                + "Enabling combiner but combine function might fail at runtime.");
                return true;
            }
        }
        return false;
    }

    @Override
    protected GroupReduceFunction<IN, OUT> getFunction() {
        return function;
    }

    // --------------------------------------------------------------------------------------------
    //  Properties
    // --------------------------------------------------------------------------------------------
    @Internal
    public boolean isCombinable() {
        return combinable;
    }

    public GroupReduceOperator<IN, OUT> setCombinable(boolean combinable) {

        if (combinable) {
            // sanity check that the function is a subclass of the combine interface
            if (!checkCombinability()) {
                throw new IllegalArgumentException(
                        "Either the function does not implement a combine interface, "
                                + "or the types of the combine() and reduce() methods are not compatible.");
            }
            this.combinable = true;
        } else {
            this.combinable = false;
        }
        return this;
    }

    @Override
    @Internal
    public SingleInputSemanticProperties getSemanticProperties() {

        SingleInputSemanticProperties props = super.getSemanticProperties();

        // offset semantic information by extracted key fields
        if (props != null
                && this.grouper != null
                && this.grouper.keys instanceof SelectorFunctionKeys) {

            int offset =
                    ((SelectorFunctionKeys<?, ?>) this.grouper.keys).getKeyType().getTotalFields();
            if (this.grouper instanceof SortedGrouping) {
                offset +=
                        ((SortedGrouping<?>) this.grouper)
                                .getSortSelectionFunctionKey()
                                .getKeyType()
                                .getTotalFields();
            }
            props =
                    SemanticPropUtil.addSourceFieldOffset(
                            props, this.getInputType().getTotalFields(), offset);
        }

        return props;
    }

    // --------------------------------------------------------------------------------------------
    //  Translation
    // --------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    protected GroupReduceOperatorBase<?, OUT, ?> translateToDataFlow(Operator<IN> input) {

        String name = getName() != null ? getName() : "GroupReduce at " + defaultName;

        // wrap CombineFunction in GroupCombineFunction if combinable
        if (combinable && function instanceof CombineFunction<?, ?>) {
            this.function =
                    function instanceof RichGroupReduceFunction<?, ?>
                            ? new RichCombineToGroupCombineWrapper(
                                    (RichGroupReduceFunction<?, ?>) function)
                            : new CombineToGroupCombineWrapper((CombineFunction<?, ?>) function);
        }

        // distinguish between grouped reduce and non-grouped reduce
        if (grouper == null) {
            // non grouped reduce
            UnaryOperatorInformation<IN, OUT> operatorInfo =
                    new UnaryOperatorInformation<>(getInputType(), getResultType());
            GroupReduceOperatorBase<IN, OUT, GroupReduceFunction<IN, OUT>> po =
                    new GroupReduceOperatorBase<>(function, operatorInfo, new int[0], name);

            po.setCombinable(combinable);
            po.setInput(input);
            // the parallelism for a non grouped reduce can only be 1
            po.setParallelism(1);
            return po;
        }

        if (grouper.getKeys() instanceof SelectorFunctionKeys) {

            @SuppressWarnings("unchecked")
            SelectorFunctionKeys<IN, ?> selectorKeys =
                    (SelectorFunctionKeys<IN, ?>) grouper.getKeys();

            if (grouper instanceof SortedGrouping) {
                SortedGrouping<IN> sortedGrouping = (SortedGrouping<IN>) grouper;
                SelectorFunctionKeys<IN, ?> sortKeys = sortedGrouping.getSortSelectionFunctionKey();
                Ordering groupOrder = sortedGrouping.getGroupOrdering();

                PlanUnwrappingSortedReduceGroupOperator<IN, OUT, ?, ?> po =
                        translateSelectorFunctionSortedReducer(
                                selectorKeys,
                                sortKeys,
                                groupOrder,
                                function,
                                getResultType(),
                                name,
                                input,
                                isCombinable());

                po.setParallelism(this.getParallelism());
                po.setCustomPartitioner(grouper.getCustomPartitioner());
                return po;
            } else {
                PlanUnwrappingReduceGroupOperator<IN, OUT, ?> po =
                        translateSelectorFunctionReducer(
                                selectorKeys,
                                function,
                                getResultType(),
                                name,
                                input,
                                isCombinable());

                po.setParallelism(this.getParallelism());
                po.setCustomPartitioner(grouper.getCustomPartitioner());
                return po;
            }
        } else if (grouper.getKeys() instanceof ExpressionKeys) {

            int[] logicalKeyPositions = grouper.getKeys().computeLogicalKeyPositions();
            UnaryOperatorInformation<IN, OUT> operatorInfo =
                    new UnaryOperatorInformation<>(getInputType(), getResultType());
            GroupReduceOperatorBase<IN, OUT, GroupReduceFunction<IN, OUT>> po =
                    new GroupReduceOperatorBase<>(
                            function, operatorInfo, logicalKeyPositions, name);

            po.setCombinable(combinable);
            po.setInput(input);
            po.setParallelism(getParallelism());
            po.setCustomPartitioner(grouper.getCustomPartitioner());

            // set group order
            if (grouper instanceof SortedGrouping) {
                SortedGrouping<IN> sortedGrouper = (SortedGrouping<IN>) grouper;

                int[] sortKeyPositions = sortedGrouper.getGroupSortKeyPositions();
                Order[] sortOrders = sortedGrouper.getGroupSortOrders();

                Ordering o = new Ordering();
                for (int i = 0; i < sortKeyPositions.length; i++) {
                    o.appendOrdering(sortKeyPositions[i], null, sortOrders[i]);
                }
                po.setGroupOrder(o);
            }

            return po;
        } else {
            throw new UnsupportedOperationException("Unrecognized key type.");
        }
    }

    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static <IN, OUT, K>
            PlanUnwrappingReduceGroupOperator<IN, OUT, K> translateSelectorFunctionReducer(
                    SelectorFunctionKeys<IN, ?> rawKeys,
                    GroupReduceFunction<IN, OUT> function,
                    TypeInformation<OUT> outputType,
                    String name,
                    Operator<IN> input,
                    boolean combinable) {
        SelectorFunctionKeys<IN, K> keys = (SelectorFunctionKeys<IN, K>) rawKeys;
        TypeInformation<Tuple2<K, IN>> typeInfoWithKey = KeyFunctions.createTypeWithKey(keys);

        Operator<Tuple2<K, IN>> keyedInput = KeyFunctions.appendKeyExtractor(input, keys);

        PlanUnwrappingReduceGroupOperator<IN, OUT, K> reducer =
                new PlanUnwrappingReduceGroupOperator(
                        function, keys, name, outputType, typeInfoWithKey, combinable);
        reducer.setInput(keyedInput);

        return reducer;
    }

    @SuppressWarnings("unchecked")
    private static <IN, OUT, K1, K2>
            PlanUnwrappingSortedReduceGroupOperator<IN, OUT, K1, K2>
                    translateSelectorFunctionSortedReducer(
                            SelectorFunctionKeys<IN, ?> rawGroupingKey,
                            SelectorFunctionKeys<IN, ?> rawSortingKey,
                            Ordering groupOrdering,
                            GroupReduceFunction<IN, OUT> function,
                            TypeInformation<OUT> outputType,
                            String name,
                            Operator<IN> input,
                            boolean combinable) {
        final SelectorFunctionKeys<IN, K1> groupingKey =
                (SelectorFunctionKeys<IN, K1>) rawGroupingKey;
        final SelectorFunctionKeys<IN, K2> sortingKey =
                (SelectorFunctionKeys<IN, K2>) rawSortingKey;
        TypeInformation<Tuple3<K1, K2, IN>> typeInfoWithKey =
                KeyFunctions.createTypeWithKey(groupingKey, sortingKey);

        Operator<Tuple3<K1, K2, IN>> inputWithKey =
                KeyFunctions.appendKeyExtractor(input, groupingKey, sortingKey);

        PlanUnwrappingSortedReduceGroupOperator<IN, OUT, K1, K2> reducer =
                new PlanUnwrappingSortedReduceGroupOperator<>(
                        function,
                        groupingKey,
                        sortingKey,
                        name,
                        outputType,
                        typeInfoWithKey,
                        combinable);
        reducer.setInput(inputWithKey);
        reducer.setGroupOrder(groupOrdering);

        return reducer;
    }
}
