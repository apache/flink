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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.multipleinput.BatchMultipleInputStreamOperatorFactory;
import org.apache.flink.table.runtime.operators.multipleinput.TableOperatorWrapperGenerator;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Batch {@link ExecNode} for multiple input which contains a sub-graph of {@link ExecNode}s. The
 * root node of the sub-graph is {@link #rootNode}, and the leaf nodes of the sub-graph are the
 * output nodes of the {@link #getInputNodes()}.
 *
 * <p>The following example shows a graph of {@code ExecNode}s with multiple input node:
 *
 * <pre>{@code
 *          Sink
 *           |
 * +---------+--------+
 * |         |        |
 * |       Join       |
 * |     /     \      | BatchExecMultipleInput
 * |   Agg1    Agg2   |
 * |    |       |     |
 * +----+-------+-----+
 *      |       |
 * Exchange1 Exchange2
 *      |       |
 *    Scan1   Scan2
 * }</pre>
 *
 * <p>The multiple input node contains three nodes: `Join`, `Agg1` and `Agg2`. `Join` is the root
 * node ({@link #rootNode}) of the sub-graph, `Agg1` and `Agg2` are the leaf nodes of the sub-graph,
 * `Exchange1` and `Exchange2` are the input nodes of the multiple input node.
 */
public class BatchExecMultipleInput extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private final ExecNode<?> rootNode;
    private final List<ExecEdge> originalEdges;

    public BatchExecMultipleInput(
            ReadableConfig tableConfig,
            List<InputProperty> inputProperties,
            ExecNode<?> rootNode,
            List<ExecEdge> originalEdges,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecMultipleInput.class),
                ExecNodeContext.newPersistedConfig(BatchExecMultipleInput.class, tableConfig),
                inputProperties,
                rootNode.getOutputType(),
                description);
        this.rootNode = rootNode;
        checkArgument(inputProperties.size() == originalEdges.size());
        this.originalEdges = originalEdges;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final List<Transformation<?>> inputTransforms = new ArrayList<>();
        for (ExecEdge inputEdge : getInputEdges()) {
            inputTransforms.add(inputEdge.translateToPlan(planner));
        }
        final Transformation<?> outputTransform = rootNode.translateToPlan(planner);
        final int[] readOrders =
                getInputProperties().stream()
                        .map(InputProperty::getPriority)
                        .mapToInt(i -> i)
                        .toArray();

        final TableOperatorWrapperGenerator generator =
                new TableOperatorWrapperGenerator(inputTransforms, outputTransform, readOrders);
        generator.generate();

        final List<Pair<Transformation<?>, InputSpec>> inputTransformAndInputSpecPairs =
                generator.getInputTransformAndInputSpecPairs();

        final MultipleInputTransformation<RowData> multipleInputTransform =
                new MultipleInputTransformation<>(
                        createTransformationName(config),
                        new BatchMultipleInputStreamOperatorFactory(
                                inputTransformAndInputSpecPairs.stream()
                                        .map(Pair::getValue)
                                        .collect(Collectors.toList()),
                                generator.getHeadWrappers(),
                                generator.getTailWrapper()),
                        InternalTypeInfo.of(getOutputType()),
                        generator.getParallelism());
        multipleInputTransform.setDescription(createTransformationDescription(config));
        inputTransformAndInputSpecPairs.forEach(
                input -> multipleInputTransform.addInput(input.getKey()));

        if (generator.getMaxParallelism() > 0) {
            multipleInputTransform.setMaxParallelism(generator.getMaxParallelism());
        }
        // set resources
        multipleInputTransform.setResources(
                generator.getMinResources(), generator.getPreferredResources());
        final int memoryWeight = generator.getManagedMemoryWeight();
        final long memoryBytes = (long) memoryWeight << 20;
        ExecNodeUtil.setManagedMemoryWeight(multipleInputTransform, memoryBytes);

        // set chaining strategy for source chaining
        multipleInputTransform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);

        return multipleInputTransform;
    }

    public List<ExecEdge> getOriginalEdges() {
        return originalEdges;
    }
}
