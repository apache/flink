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

package org.apache.flink.api.common.operators.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.CopyingListCollector;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.ListKeyGroupedIterator;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Base operator for the combineGroup transformation. It receives the UDF GroupCombineFunction as an
 * input. This class is later processed by the compiler to generate the plan.
 *
 * @see org.apache.flink.api.common.functions.CombineFunction
 */
@Internal
public class GroupCombineOperatorBase<IN, OUT, FT extends GroupCombineFunction<IN, OUT>>
        extends SingleInputOperator<IN, OUT, FT> {

    /** The ordering for the order inside a reduce group. */
    private Ordering groupOrder;

    public GroupCombineOperatorBase(
            FT udf,
            UnaryOperatorInformation<IN, OUT> operatorInfo,
            int[] keyPositions,
            String name) {
        super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, keyPositions, name);
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Sets the order of the elements within a reduce group.
     *
     * @param order The order for the elements in a reduce group.
     */
    public void setGroupOrder(Ordering order) {
        this.groupOrder = order;
    }

    /**
     * Gets the order of elements within a reduce group. If no such order has been set, this method
     * returns null.
     *
     * @return The secondary order.
     */
    public Ordering getGroupOrder() {
        return this.groupOrder;
    }

    private TypeComparator<IN> getTypeComparator(
            TypeInformation<IN> typeInfo,
            int[] sortColumns,
            boolean[] sortOrderings,
            ExecutionConfig executionConfig) {
        if (typeInfo instanceof CompositeType) {
            return ((CompositeType<IN>) typeInfo)
                    .createComparator(sortColumns, sortOrderings, 0, executionConfig);
        } else if (typeInfo instanceof AtomicType) {
            return ((AtomicType<IN>) typeInfo).createComparator(sortOrderings[0], executionConfig);
        }

        throw new InvalidProgramException(
                "Input type of GroupCombine must be one of composite types or atomic types.");
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected List<OUT> executeOnCollections(
            List<IN> inputData, RuntimeContext ctx, ExecutionConfig executionConfig)
            throws Exception {
        GroupCombineFunction<IN, OUT> function = this.userFunction.getUserCodeObject();

        UnaryOperatorInformation<IN, OUT> operatorInfo = getOperatorInfo();
        TypeInformation<IN> inputType = operatorInfo.getInputType();

        int[] keyColumns = getKeyColumns(0);
        int[] sortColumns = keyColumns;
        boolean[] sortOrderings = new boolean[sortColumns.length];

        if (groupOrder != null) {
            sortColumns = ArrayUtils.addAll(sortColumns, groupOrder.getFieldPositions());
            sortOrderings = ArrayUtils.addAll(sortOrderings, groupOrder.getFieldSortDirections());
        }

        if (sortColumns.length == 0) { // => all reduce. No comparator
            checkArgument(sortOrderings.length == 0);
        } else {
            final TypeComparator<IN> sortComparator =
                    getTypeComparator(inputType, sortColumns, sortOrderings, executionConfig);

            Collections.sort(
                    inputData,
                    new Comparator<IN>() {
                        @Override
                        public int compare(IN o1, IN o2) {
                            return sortComparator.compare(o1, o2);
                        }
                    });
        }

        FunctionUtils.setFunctionRuntimeContext(function, ctx);
        FunctionUtils.openFunction(function, DefaultOpenContext.INSTANCE);

        ArrayList<OUT> result = new ArrayList<OUT>();

        if (keyColumns.length == 0) {
            final TypeSerializer<IN> inputSerializer = inputType.createSerializer(executionConfig);
            TypeSerializer<OUT> outSerializer =
                    getOperatorInfo().getOutputType().createSerializer(executionConfig);
            List<IN> inputDataCopy = new ArrayList<IN>(inputData.size());
            for (IN in : inputData) {
                inputDataCopy.add(inputSerializer.copy(in));
            }
            CopyingListCollector<OUT> collector =
                    new CopyingListCollector<OUT>(result, outSerializer);

            function.combine(inputDataCopy, collector);
        } else {
            final TypeSerializer<IN> inputSerializer = inputType.createSerializer(executionConfig);
            boolean[] keyOrderings = new boolean[keyColumns.length];
            final TypeComparator<IN> comparator =
                    getTypeComparator(inputType, keyColumns, keyOrderings, executionConfig);

            ListKeyGroupedIterator<IN> keyedIterator =
                    new ListKeyGroupedIterator<IN>(inputData, inputSerializer, comparator);

            TypeSerializer<OUT> outSerializer =
                    getOperatorInfo().getOutputType().createSerializer(executionConfig);
            CopyingListCollector<OUT> collector =
                    new CopyingListCollector<OUT>(result, outSerializer);

            while (keyedIterator.nextKey()) {
                function.combine(keyedIterator.getValues(), collector);
            }
        }

        FunctionUtils.closeFunction(function);
        return result;
    }
}
