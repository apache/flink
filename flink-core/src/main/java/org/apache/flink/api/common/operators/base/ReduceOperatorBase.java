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
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.TypeComparable;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base data flow operator for Reduce user-defined functions. Accepts reduce functions and key
 * positions. The key positions are expected in the flattened common data model.
 *
 * @see org.apache.flink.api.common.functions.ReduceFunction
 * @param <T> The type (parameters and return type) of the reduce function.
 * @param <FT> The type of the reduce function.
 */
@Internal
public class ReduceOperatorBase<T, FT extends ReduceFunction<T>>
        extends SingleInputOperator<T, T, FT> {

    /**
     * An enumeration of hints, optionally usable to tell the system exactly how to execute the
     * combiner phase of a reduce. (Note: The final reduce phase (after combining) is currently
     * always executed by a sort-based strategy.)
     */
    public enum CombineHint {

        /**
         * Leave the choice how to do the combine to the optimizer. (This currently defaults to
         * SORT.)
         */
        OPTIMIZER_CHOOSES,

        /** Use a sort-based strategy. */
        SORT,

        /**
         * Use a hash-based strategy. This should be faster in most cases, especially if the number
         * of different keys is small compared to the number of input elements (eg. 1/10).
         */
        HASH,

        /** Disable the use of a combiner. */
        NONE
    }

    private CombineHint hint;

    private Partitioner<?> customPartitioner;

    /**
     * Creates a grouped reduce data flow operator.
     *
     * @param udf The user-defined function, contained in the UserCodeWrapper.
     * @param operatorInfo The type information, describing input and output types of the reduce
     *     function.
     * @param keyPositions The positions of the key fields, in the common data model (flattened).
     * @param name The name of the operator (for logging and messages).
     */
    public ReduceOperatorBase(
            UserCodeWrapper<FT> udf,
            UnaryOperatorInformation<T, T> operatorInfo,
            int[] keyPositions,
            String name) {
        super(udf, operatorInfo, keyPositions, name);
    }

    /**
     * Creates a grouped reduce data flow operator.
     *
     * @param udf The user-defined function, as a function object.
     * @param operatorInfo The type information, describing input and output types of the reduce
     *     function.
     * @param keyPositions The positions of the key fields, in the common data model (flattened).
     * @param name The name of the operator (for logging and messages).
     */
    public ReduceOperatorBase(
            FT udf, UnaryOperatorInformation<T, T> operatorInfo, int[] keyPositions, String name) {
        super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, keyPositions, name);
    }

    /**
     * Creates a grouped reduce data flow operator.
     *
     * @param udf The class representing the parameterless user-defined function.
     * @param operatorInfo The type information, describing input and output types of the reduce
     *     function.
     * @param keyPositions The positions of the key fields, in the common data model (flattened).
     * @param name The name of the operator (for logging and messages).
     */
    public ReduceOperatorBase(
            Class<? extends FT> udf,
            UnaryOperatorInformation<T, T> operatorInfo,
            int[] keyPositions,
            String name) {
        super(new UserCodeClassWrapper<FT>(udf), operatorInfo, keyPositions, name);
    }

    // --------------------------------------------------------------------------------------------
    //  Non-grouped reduce operations
    // --------------------------------------------------------------------------------------------

    /**
     * Creates a non-grouped reduce data flow operator (all-reduce).
     *
     * @param udf The user-defined function, contained in the UserCodeWrapper.
     * @param operatorInfo The type information, describing input and output types of the reduce
     *     function.
     * @param name The name of the operator (for logging and messages).
     */
    public ReduceOperatorBase(
            UserCodeWrapper<FT> udf, UnaryOperatorInformation<T, T> operatorInfo, String name) {
        super(udf, operatorInfo, name);
    }

    /**
     * Creates a non-grouped reduce data flow operator (all-reduce).
     *
     * @param udf The user-defined function, as a function object.
     * @param operatorInfo The type information, describing input and output types of the reduce
     *     function.
     * @param name The name of the operator (for logging and messages).
     */
    public ReduceOperatorBase(FT udf, UnaryOperatorInformation<T, T> operatorInfo, String name) {
        super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, name);
    }

    /**
     * Creates a non-grouped reduce data flow operator (all-reduce).
     *
     * @param udf The class representing the parameterless user-defined function.
     * @param operatorInfo The type information, describing input and output types of the reduce
     *     function.
     * @param name The name of the operator (for logging and messages).
     */
    public ReduceOperatorBase(
            Class<? extends FT> udf, UnaryOperatorInformation<T, T> operatorInfo, String name) {
        super(new UserCodeClassWrapper<FT>(udf), operatorInfo, name);
    }

    // --------------------------------------------------------------------------------------------

    public void setCustomPartitioner(Partitioner<?> customPartitioner) {
        if (customPartitioner != null) {
            int[] keys = getKeyColumns(0);
            if (keys == null || keys.length == 0) {
                throw new IllegalArgumentException(
                        "Cannot use custom partitioner for a non-grouped GroupReduce (AllGroupReduce)");
            }
            if (keys.length > 1) {
                throw new IllegalArgumentException(
                        "Cannot use the key partitioner for composite keys (more than one key field)");
            }
        }
        this.customPartitioner = customPartitioner;
    }

    public Partitioner<?> getCustomPartitioner() {
        return customPartitioner;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected List<T> executeOnCollections(
            List<T> inputData, RuntimeContext ctx, ExecutionConfig executionConfig)
            throws Exception {
        // make sure we can handle empty inputs
        if (inputData.isEmpty()) {
            return Collections.emptyList();
        }

        ReduceFunction<T> function = this.userFunction.getUserCodeObject();

        UnaryOperatorInformation<T, T> operatorInfo = getOperatorInfo();
        TypeInformation<T> inputType = operatorInfo.getInputType();

        int[] inputColumns = getKeyColumns(0);

        if (!(inputType instanceof CompositeType) && inputColumns.length > 1) {
            throw new InvalidProgramException("Grouping is only possible on composite types.");
        }

        FunctionUtils.setFunctionRuntimeContext(function, ctx);
        FunctionUtils.openFunction(function, DefaultOpenContext.INSTANCE);

        TypeSerializer<T> serializer =
                getOperatorInfo().getInputType().createSerializer(executionConfig);

        if (inputColumns.length > 0) {
            boolean[] inputOrderings = new boolean[inputColumns.length];
            TypeComparator<T> inputComparator =
                    inputType instanceof AtomicType
                            ? ((AtomicType<T>) inputType).createComparator(false, executionConfig)
                            : ((CompositeType<T>) inputType)
                                    .createComparator(
                                            inputColumns, inputOrderings, 0, executionConfig);

            Map<TypeComparable<T>, T> aggregateMap =
                    new HashMap<TypeComparable<T>, T>(inputData.size() / 10);

            for (T next : inputData) {
                TypeComparable<T> wrapper = new TypeComparable<T>(next, inputComparator);

                T existing = aggregateMap.get(wrapper);
                T result;

                if (existing != null) {
                    result = function.reduce(existing, serializer.copy(next));
                } else {
                    result = next;
                }

                result = serializer.copy(result);

                aggregateMap.put(wrapper, result);
            }

            FunctionUtils.closeFunction(function);
            return new ArrayList<T>(aggregateMap.values());
        } else {
            T aggregate = inputData.get(0);

            aggregate = serializer.copy(aggregate);

            for (int i = 1; i < inputData.size(); i++) {
                T next = function.reduce(aggregate, serializer.copy(inputData.get(i)));
                aggregate = serializer.copy(next);
            }

            FunctionUtils.setFunctionRuntimeContext(function, ctx);

            return Collections.singletonList(aggregate);
        }
    }

    public void setCombineHint(CombineHint hint) {
        if (hint == null) {
            throw new IllegalArgumentException("Reduce Hint must not be null.");
        }
        this.hint = hint;
    }

    public CombineHint getCombineHint() {
        return hint;
    }
}
