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
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.CopyingListCollector;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/** @see org.apache.flink.api.common.functions.CoGroupFunction */
@Internal
public class CoGroupRawOperatorBase<IN1, IN2, OUT, FT extends CoGroupFunction<IN1, IN2, OUT>>
        extends DualInputOperator<IN1, IN2, OUT, FT> {

    /** The ordering for the order inside a group from input one. */
    private Ordering groupOrder1;

    /** The ordering for the order inside a group from input two. */
    private Ordering groupOrder2;

    // --------------------------------------------------------------------------------------------
    private boolean combinableFirst;

    private boolean combinableSecond;

    public CoGroupRawOperatorBase(
            UserCodeWrapper<FT> udf,
            BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo,
            int[] keyPositions1,
            int[] keyPositions2,
            String name) {
        super(udf, operatorInfo, keyPositions1, keyPositions2, name);
        this.combinableFirst = false;
        this.combinableSecond = false;
    }

    public CoGroupRawOperatorBase(
            FT udf,
            BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo,
            int[] keyPositions1,
            int[] keyPositions2,
            String name) {
        this(new UserCodeObjectWrapper<FT>(udf), operatorInfo, keyPositions1, keyPositions2, name);
    }

    public CoGroupRawOperatorBase(
            Class<? extends FT> udf,
            BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo,
            int[] keyPositions1,
            int[] keyPositions2,
            String name) {
        this(new UserCodeClassWrapper<FT>(udf), operatorInfo, keyPositions1, keyPositions2, name);
    }

    // --------------------------------------------------------------------------------------------
    /**
     * Sets the order of the elements within a group for the given input.
     *
     * @param inputNum The number of the input (here either <i>0</i> or <i>1</i>).
     * @param order The order for the elements in a group.
     */
    public void setGroupOrder(int inputNum, Ordering order) {
        if (inputNum == 0) {
            this.groupOrder1 = order;
        } else if (inputNum == 1) {
            this.groupOrder2 = order;
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * Sets the order of the elements within a group for the first input.
     *
     * @param order The order for the elements in a group.
     */
    public void setGroupOrderForInputOne(Ordering order) {
        setGroupOrder(0, order);
    }

    /**
     * Sets the order of the elements within a group for the second input.
     *
     * @param order The order for the elements in a group.
     */
    public void setGroupOrderForInputTwo(Ordering order) {
        setGroupOrder(1, order);
    }

    /**
     * Gets the value order for an input, i.e. the order of elements within a group. If no such
     * order has been set, this method returns null.
     *
     * @param inputNum The number of the input (here either <i>0</i> or <i>1</i>).
     * @return The group order.
     */
    public Ordering getGroupOrder(int inputNum) {
        if (inputNum == 0) {
            return this.groupOrder1;
        } else if (inputNum == 1) {
            return this.groupOrder2;
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * Gets the order of elements within a group for the first input. If no such order has been set,
     * this method returns null.
     *
     * @return The group order for the first input.
     */
    public Ordering getGroupOrderForInputOne() {
        return getGroupOrder(0);
    }

    /**
     * Gets the order of elements within a group for the second input. If no such order has been
     * set, this method returns null.
     *
     * @return The group order for the second input.
     */
    public Ordering getGroupOrderForInputTwo() {
        return getGroupOrder(1);
    }

    // --------------------------------------------------------------------------------------------
    public boolean isCombinableFirst() {
        return this.combinableFirst;
    }

    public void setCombinableFirst(boolean combinableFirst) {
        this.combinableFirst = combinableFirst;
    }

    public boolean isCombinableSecond() {
        return this.combinableSecond;
    }

    public void setCombinableSecond(boolean combinableSecond) {
        this.combinableSecond = combinableSecond;
    }

    // ------------------------------------------------------------------------
    @Override
    protected List<OUT> executeOnCollections(
            List<IN1> input1, List<IN2> input2, RuntimeContext ctx, ExecutionConfig executionConfig)
            throws Exception {
        // --------------------------------------------------------------------
        // Setup
        // --------------------------------------------------------------------
        TypeInformation<IN1> inputType1 = getOperatorInfo().getFirstInputType();
        TypeInformation<IN2> inputType2 = getOperatorInfo().getSecondInputType();

        int[] inputKeys1 = getKeyColumns(0);
        int[] inputKeys2 = getKeyColumns(1);

        boolean[] inputSortDirections1 = new boolean[inputKeys1.length];
        boolean[] inputSortDirections2 = new boolean[inputKeys2.length];

        Arrays.fill(inputSortDirections1, true);
        Arrays.fill(inputSortDirections2, true);

        final TypeSerializer<IN1> inputSerializer1 = inputType1.createSerializer(executionConfig);
        final TypeSerializer<IN2> inputSerializer2 = inputType2.createSerializer(executionConfig);

        final TypeComparator<IN1> inputComparator1 =
                getTypeComparator(executionConfig, inputType1, inputKeys1, inputSortDirections1);
        final TypeComparator<IN2> inputComparator2 =
                getTypeComparator(executionConfig, inputType2, inputKeys2, inputSortDirections2);

        SimpleListIterable<IN1> iterator1 =
                new SimpleListIterable<IN1>(input1, inputComparator1, inputSerializer1);
        SimpleListIterable<IN2> iterator2 =
                new SimpleListIterable<IN2>(input2, inputComparator2, inputSerializer2);

        // --------------------------------------------------------------------
        // Run UDF
        // --------------------------------------------------------------------
        CoGroupFunction<IN1, IN2, OUT> function = userFunction.getUserCodeObject();

        FunctionUtils.setFunctionRuntimeContext(function, ctx);
        FunctionUtils.openFunction(function, DefaultOpenContext.INSTANCE);

        List<OUT> result = new ArrayList<OUT>();
        Collector<OUT> resultCollector =
                new CopyingListCollector<OUT>(
                        result,
                        getOperatorInfo().getOutputType().createSerializer(executionConfig));

        function.coGroup(iterator1, iterator2, resultCollector);

        FunctionUtils.closeFunction(function);

        return result;
    }

    private <T> TypeComparator<T> getTypeComparator(
            ExecutionConfig executionConfig,
            TypeInformation<T> inputType,
            int[] inputKeys,
            boolean[] inputSortDirections) {
        if (!(inputType instanceof CompositeType)) {
            throw new InvalidProgramException("Input types of coGroup must be composite types.");
        }

        return ((CompositeType<T>) inputType)
                .createComparator(inputKeys, inputSortDirections, 0, executionConfig);
    }

    public static class SimpleListIterable<IN> implements Iterable<IN> {
        private List<IN> values;
        private TypeSerializer<IN> serializer;
        private boolean copy;

        public SimpleListIterable(
                List<IN> values, final TypeComparator<IN> comparator, TypeSerializer<IN> serializer)
                throws IOException {
            this.values = values;
            this.serializer = serializer;

            Collections.sort(
                    values,
                    new Comparator<IN>() {
                        @Override
                        public int compare(IN o1, IN o2) {
                            return comparator.compare(o1, o2);
                        }
                    });
        }

        @Override
        public Iterator<IN> iterator() {
            return new SimpleListIterator<IN>(values, serializer);
        }

        protected class SimpleListIterator<IN> implements Iterator<IN> {
            private final List<IN> values;
            private final TypeSerializer<IN> serializer;
            private int pos = 0;

            public SimpleListIterator(List<IN> values, TypeSerializer<IN> serializer) {
                this.values = values;
                this.serializer = serializer;
            }

            @Override
            public boolean hasNext() {
                return pos < values.size();
            }

            @Override
            public IN next() {
                IN current = values.get(pos++);
                return serializer.copy(current);
            }

            @Override
            public void remove() { // unused
            }
        }
    }
}
