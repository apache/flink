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
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.CopyingListCollector;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.GenericPairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** @see org.apache.flink.api.common.functions.FlatJoinFunction */
@Internal
public class InnerJoinOperatorBase<IN1, IN2, OUT, FT extends FlatJoinFunction<IN1, IN2, OUT>>
        extends JoinOperatorBase<IN1, IN2, OUT, FT> {

    public InnerJoinOperatorBase(
            UserCodeWrapper<FT> udf,
            BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo,
            int[] keyPositions1,
            int[] keyPositions2,
            String name) {
        super(udf, operatorInfo, keyPositions1, keyPositions2, name);
    }

    public InnerJoinOperatorBase(
            FT udf,
            BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo,
            int[] keyPositions1,
            int[] keyPositions2,
            String name) {
        super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, keyPositions1, keyPositions2, name);
    }

    public InnerJoinOperatorBase(
            Class<? extends FT> udf,
            BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo,
            int[] keyPositions1,
            int[] keyPositions2,
            String name) {
        super(new UserCodeClassWrapper<FT>(udf), operatorInfo, keyPositions1, keyPositions2, name);
    }

    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    @Override
    protected List<OUT> executeOnCollections(
            List<IN1> inputData1,
            List<IN2> inputData2,
            RuntimeContext runtimeContext,
            ExecutionConfig executionConfig)
            throws Exception {
        FlatJoinFunction<IN1, IN2, OUT> function = userFunction.getUserCodeObject();

        FunctionUtils.setFunctionRuntimeContext(function, runtimeContext);
        FunctionUtils.openFunction(function, this.parameters);

        TypeInformation<IN1> leftInformation = getOperatorInfo().getFirstInputType();
        TypeInformation<IN2> rightInformation = getOperatorInfo().getSecondInputType();
        TypeInformation<OUT> outInformation = getOperatorInfo().getOutputType();

        TypeSerializer<IN1> leftSerializer = leftInformation.createSerializer(executionConfig);
        TypeSerializer<IN2> rightSerializer = rightInformation.createSerializer(executionConfig);

        TypeComparator<IN1> leftComparator;
        TypeComparator<IN2> rightComparator;

        if (leftInformation instanceof AtomicType) {
            leftComparator =
                    ((AtomicType<IN1>) leftInformation).createComparator(true, executionConfig);
        } else if (leftInformation instanceof CompositeType) {
            int[] keyPositions = getKeyColumns(0);
            boolean[] orders = new boolean[keyPositions.length];
            Arrays.fill(orders, true);

            leftComparator =
                    ((CompositeType<IN1>) leftInformation)
                            .createComparator(keyPositions, orders, 0, executionConfig);
        } else {
            throw new RuntimeException(
                    "Type information for left input of type "
                            + leftInformation.getClass().getCanonicalName()
                            + " is not supported. Could not generate a comparator.");
        }

        if (rightInformation instanceof AtomicType) {
            rightComparator =
                    ((AtomicType<IN2>) rightInformation).createComparator(true, executionConfig);
        } else if (rightInformation instanceof CompositeType) {
            int[] keyPositions = getKeyColumns(1);
            boolean[] orders = new boolean[keyPositions.length];
            Arrays.fill(orders, true);

            rightComparator =
                    ((CompositeType<IN2>) rightInformation)
                            .createComparator(keyPositions, orders, 0, executionConfig);
        } else {
            throw new RuntimeException(
                    "Type information for right input of type "
                            + rightInformation.getClass().getCanonicalName()
                            + " is not supported. Could not generate a comparator.");
        }

        TypePairComparator<IN1, IN2> pairComparator =
                new GenericPairComparator<IN1, IN2>(leftComparator, rightComparator);

        List<OUT> result = new ArrayList<OUT>();
        Collector<OUT> collector =
                new CopyingListCollector<OUT>(
                        result, outInformation.createSerializer(executionConfig));

        Map<Integer, List<IN2>> probeTable = new HashMap<Integer, List<IN2>>();

        // Build hash table
        for (IN2 element : inputData2) {
            List<IN2> list = probeTable.get(rightComparator.hash(element));
            if (list == null) {
                list = new ArrayList<IN2>();
                probeTable.put(rightComparator.hash(element), list);
            }

            list.add(element);
        }

        // Probing
        for (IN1 left : inputData1) {
            List<IN2> matchingHashes = probeTable.get(leftComparator.hash(left));

            if (matchingHashes != null) {
                pairComparator.setReference(left);
                for (IN2 right : matchingHashes) {
                    if (pairComparator.equalToReference(right)) {
                        function.join(
                                leftSerializer.copy(left), rightSerializer.copy(right), collector);
                    }
                }
            }
        }

        FunctionUtils.closeFunction(function);

        return result;
    }
}
