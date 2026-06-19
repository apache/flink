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
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.adaptive.AdaptiveJoinOperatorGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.OperatorType;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.adaptive.AdaptiveJoinOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** {@link BatchExecNode} for adaptive join. */
public class BatchExecAdaptiveJoin extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private final JoinSpec joinSpec;
    private final boolean leftIsBuild;
    private final int estimatedLeftAvgRowSize;
    private final int estimatedRightAvgRowSize;
    private final long estimatedLeftRowCount;
    private final long estimatedRightRowCount;
    private final boolean tryDistinctBuildRow;
    private final String description;
    private final OperatorType originalJoin;

    public BatchExecAdaptiveJoin(
            ReadableConfig tableConfig,
            JoinSpec joinSpec,
            int estimatedLeftAvgRowSize,
            int estimatedRightAvgRowSize,
            long estimatedLeftRowCount,
            long estimatedRightRowCount,
            boolean leftIsBuild,
            boolean tryDistinctBuildRow,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description,
            OperatorType originalJoin) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecAdaptiveJoin.class),
                ExecNodeContext.newPersistedConfig(BatchExecAdaptiveJoin.class, tableConfig),
                inputProperties,
                outputType,
                description);
        this.joinSpec = joinSpec;
        this.estimatedLeftAvgRowSize = estimatedLeftAvgRowSize;
        this.estimatedRightAvgRowSize = estimatedRightAvgRowSize;
        this.estimatedLeftRowCount = estimatedLeftRowCount;
        this.estimatedRightRowCount = estimatedRightRowCount;
        this.leftIsBuild = leftIsBuild;
        this.tryDistinctBuildRow = tryDistinctBuildRow;
        this.description = description;
        checkState(
                originalJoin == OperatorType.ShuffleHashJoin
                        || originalJoin == OperatorType.SortMergeJoin,
                String.format(
                        "Adaptive join "
                                + "currently only supports adaptive optimization for ShuffleHashJoin and "
                                + "SortMergeJoin, not including %s.",
                        originalJoin.toString()));
        this.originalJoin = originalJoin;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        ExecEdge leftInputEdge = getInputEdges().get(0);
        ExecEdge rightInputEdge = getInputEdges().get(1);

        Transformation<RowData> leftInputTransform =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        Transformation<RowData> rightInputTransform =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);
        // get input types
        RowType leftType = (RowType) leftInputEdge.getOutputType();
        RowType rightType = (RowType) rightInputEdge.getOutputType();
        long managedMemory = JoinUtil.getManagedMemory(joinSpec.getJoinType(), config);
        GeneratedJoinCondition condFunc =
                JoinUtil.generateConditionFunction(
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        joinSpec.getNonEquiCondition().orElse(null),
                        leftType,
                        rightType);

        AdaptiveJoinOperatorGenerator adaptiveJoinGenerator =
                new AdaptiveJoinOperatorGenerator(
                        joinSpec.getLeftKeys(),
                        joinSpec.getRightKeys(),
                        joinSpec.getFilterNulls(),
                        leftType,
                        rightType,
                        condFunc,
                        estimatedLeftAvgRowSize,
                        estimatedRightAvgRowSize,
                        estimatedLeftRowCount,
                        estimatedRightRowCount,
                        tryDistinctBuildRow,
                        managedMemory);

        return ExecNodeUtil.createTwoInputTransformation(
                leftInputTransform,
                rightInputTransform,
                createTransformationName(config),
                createTransformationDescription(config),
                getAdaptiveJoinOperatorFactory(
                        adaptiveJoinGenerator,
                        config.get(CoreOptions.CHECK_LEAKED_CLASSLOADER),
                        joinSpec.getJoinType(),
                        originalJoin,
                        leftIsBuild),
                InternalTypeInfo.of(getOutputType()),
                // Given that the probe side might be decided at runtime, we choose the larger
                // parallelism here.
                Math.max(leftInputTransform.getParallelism(), rightInputTransform.getParallelism()),
                managedMemory,
                false);
    }

    private StreamOperatorFactory<RowData> getAdaptiveJoinOperatorFactory(
            AdaptiveJoinOperatorGenerator adaptiveJoinGenerator,
            boolean checkClassLoaderLeak,
            FlinkJoinType joinType,
            OperatorType originalJoin,
            boolean leftIsBuild) {
        try {
            byte[] adaptiveJoinGeneratorSerialized =
                    InstantiationUtil.serializeObject(adaptiveJoinGenerator);
            return new AdaptiveJoinOperatorFactory<>(
                    adaptiveJoinGeneratorSerialized,
                    joinType,
                    originalJoin == OperatorType.SortMergeJoin,
                    leftIsBuild,
                    checkClassLoaderLeak);
        } catch (IOException e) {
            throw new TableException("The adaptive join operator failed to serialize.", e);
        }
    }

    @Override
    public String getDescription() {
        return "AdaptiveJoin("
                + "originalJoin=["
                + originalJoin
                + "], "
                + description.substring(description.indexOf('(') + 1);
    }
}
