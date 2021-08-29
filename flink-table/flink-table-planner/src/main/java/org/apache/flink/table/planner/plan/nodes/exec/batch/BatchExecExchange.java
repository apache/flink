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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.HashCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty.HashDistribution;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty.RequiredDistribution;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExchange;
import org.apache.flink.table.runtime.partitioner.BinaryHashPartitioner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.apache.flink.table.planner.utils.StreamExchangeModeUtils.getBatchStreamExchangeMode;

/**
 * This {@link ExecNode} represents a change of partitioning of the input elements for batch.
 *
 * <p>TODO Remove this class once FLINK-21224 is finished.
 */
public class BatchExecExchange extends CommonExecExchange implements BatchExecNode<RowData> {
    // the required exchange mode for reusable BatchExecExchange
    // if it's None, use value from configuration
    @Nullable private StreamExchangeMode requiredExchangeMode;

    public BatchExecExchange(InputProperty inputProperty, RowType outputType, String description) {
        super(getNewNodeId(), Collections.singletonList(inputProperty), outputType, description);
    }

    public void setRequiredExchangeMode(@Nullable StreamExchangeMode requiredExchangeMode) {
        this.requiredExchangeMode = requiredExchangeMode;
    }

    @Override
    public String getDescription() {
        // make sure the description be consistent with before, update this once plan is stable
        RequiredDistribution requiredDistribution =
                getInputProperties().get(0).getRequiredDistribution();
        StringBuilder sb = new StringBuilder();
        String type = requiredDistribution.getType().name().toLowerCase();
        if (type.equals("singleton")) {
            type = "single";
        }
        sb.append("distribution=[").append(type);
        if (requiredDistribution.getType() == InputProperty.DistributionType.HASH) {
            RowType inputRowType = (RowType) getInputEdges().get(0).getOutputType();
            HashDistribution hashDistribution = (HashDistribution) requiredDistribution;
            String[] fieldNames =
                    Arrays.stream(hashDistribution.getKeys())
                            .mapToObj(i -> inputRowType.getFieldNames().get(i))
                            .toArray(String[]::new);
            sb.append("[").append(String.join(", ", fieldNames)).append("]");
        }
        sb.append("]");
        if (requiredExchangeMode == StreamExchangeMode.BATCH) {
            sb.append(", shuffle_mode=[BATCH]");
        }
        return String.format("Exchange(%s)", sb.toString());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final StreamPartitioner<RowData> partitioner;
        final int parallelism;
        final InputProperty inputProperty = getInputProperties().get(0);
        final InputProperty.DistributionType distributionType =
                inputProperty.getRequiredDistribution().getType();
        switch (distributionType) {
            case ANY:
                partitioner = null;
                parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
                break;
            case BROADCAST:
                partitioner = new BroadcastPartitioner<>();
                parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
                break;
            case SINGLETON:
                partitioner = new GlobalPartitioner<>();
                parallelism = 1;
                break;
            case HASH:
                int[] keys = ((HashDistribution) inputProperty.getRequiredDistribution()).getKeys();
                RowType inputType = (RowType) inputEdge.getOutputType();
                String[] fieldNames =
                        Arrays.stream(keys)
                                .mapToObj(i -> inputType.getFieldNames().get(i))
                                .toArray(String[]::new);
                partitioner =
                        new BinaryHashPartitioner(
                                HashCodeGenerator.generateRowHash(
                                        new CodeGeneratorContext(planner.getTableConfig()),
                                        inputEdge.getOutputType(),
                                        "HashPartitioner",
                                        keys),
                                fieldNames);
                parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
                break;
            default:
                throw new TableException(distributionType + "is not supported now!");
        }

        final StreamExchangeMode exchangeMode =
                getBatchStreamExchangeMode(planner.getConfiguration(), requiredExchangeMode);
        final Transformation<RowData> transformation =
                new PartitionTransformation<>(inputTransform, partitioner, exchangeMode);
        transformation.setParallelism(parallelism);
        transformation.setOutputType(InternalTypeInfo.of(getOutputType()));
        return transformation;
    }

    @VisibleForTesting
    public Optional<StreamExchangeMode> getRequiredExchangeMode() {
        return Optional.ofNullable(requiredExchangeMode);
    }
}
