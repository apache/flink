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

package org.apache.flink.table.planner.plan.nodes.exec.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** An Utility class that helps translating {@link ExecNode} to {@link Transformation}. */
public class ExecNodeUtil {
    /**
     * Set memoryBytes to {@link
     * Transformation#declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase, int)}.
     */
    public static <T> void setManagedMemoryWeight(
            Transformation<T> transformation, long memoryBytes) {
        // Using Bytes can easily overflow
        // Using KibiBytes to cast to int
        // Careful about zero
        if (memoryBytes > 0) {
            int memoryKibiBytes = (int) Math.max(1, (memoryBytes >> 10));
            Optional<Integer> previousWeight =
                    transformation.declareManagedMemoryUseCaseAtOperatorScope(
                            ManagedMemoryUseCase.OPERATOR, memoryKibiBytes);
            if (previousWeight.isPresent()) {
                throw new TableException(
                        "Managed memory weight has been set, this should not happen.");
            }
        }
    }

    /** Create a {@link OneInputTransformation} with memoryBytes. */
    public static <T> OneInputTransformation<T, T> createOneInputTransformation(
            Transformation<T> input,
            String name,
            StreamOperatorFactory<T> operatorFactory,
            TypeInformation<T> outputType,
            int parallelism,
            long memoryBytes) {
        OneInputTransformation<T, T> transformation =
                new OneInputTransformation<>(input, name, operatorFactory, outputType, parallelism);
        setManagedMemoryWeight(transformation, memoryBytes);
        return transformation;
    }

    /** Create a {@link TwoInputTransformation} with memoryBytes. */
    public static <T> TwoInputTransformation<T, T, T> createTwoInputTransformation(
            Transformation<T> input1,
            Transformation<T> input2,
            String name,
            StreamOperatorFactory<T> operatorFactory,
            TypeInformation<T> outputType,
            int parallelism,
            long memoryBytes) {
        TwoInputTransformation<T, T, T> transformation =
                new TwoInputTransformation<>(
                        input1, input2, name, operatorFactory, outputType, parallelism);
        setManagedMemoryWeight(transformation, memoryBytes);
        return transformation;
    }

    /** Return description for multiple input node. */
    public static String getMultipleInputDescription(
            ExecNode<?> rootNode,
            List<ExecNode<?>> inputNodes,
            List<InputProperty> inputProperties) {
        String members =
                ExecNodePlanDumper.treeToString(rootNode, inputNodes, true).replace("\n", "\\n");
        StringBuilder sb = new StringBuilder();
        sb.append("MultipleInput(");
        List<String> readOrders =
                inputProperties.stream()
                        .map(InputProperty::getPriority)
                        .map(Object::toString)
                        .collect(Collectors.toList());
        boolean hasDiffReadOrder = readOrders.stream().distinct().count() > 1;
        if (hasDiffReadOrder) {
            sb.append("readOrder=[").append(String.join(",", readOrders)).append("], ");
        }
        sb.append("members=[\\n").append(members).append("]");
        sb.append(")");
        return sb.toString();
    }
}
