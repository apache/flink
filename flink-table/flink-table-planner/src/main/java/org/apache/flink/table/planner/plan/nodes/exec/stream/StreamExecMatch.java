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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecMatch;
import org.apache.flink.table.planner.plan.nodes.exec.spec.MatchSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/** Stream {@link ExecNode} which matches along with MATCH_RECOGNIZE. */
@ExecNodeMetadata(
        name = "stream-exec-match",
        version = 1,
        producedTransformations = {
            CommonExecMatch.TIMESTAMP_INSERTER_TRANSFORMATION,
            CommonExecMatch.MATCH_TRANSFORMATION
        },
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecMatch extends CommonExecMatch
        implements StreamExecNode<RowData>, MultipleTransformationTranslator<RowData> {

    public StreamExecMatch(
            ReadableConfig tableConfig,
            MatchSpec matchSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecMatch.class),
                ExecNodeContext.newPersistedConfig(StreamExecMatch.class, tableConfig),
                matchSpec,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecMatch(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_MATCH_SPEC) MatchSpec matchSpec,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, matchSpec, inputProperties, outputType, description);
    }

    @Override
    public void checkOrderKeys(RowType inputRowType) {
        SortSpec orderKeys = matchSpec.getOrderKeys();
        if (orderKeys.getFieldSize() == 0) {
            throw new TableException("You must specify either rowtime or proctime for order by.");
        }

        SortSpec.SortFieldSpec timeOrderField = orderKeys.getFieldSpec(0);
        int timeOrderFieldIdx = timeOrderField.getFieldIndex();
        LogicalType timeOrderFieldType = inputRowType.getTypeAt(timeOrderFieldIdx);
        // need to identify time between others order fields. Time needs to be first sort element
        if (!TypeCheckUtils.isRowTime(timeOrderFieldType)
                && !TypeCheckUtils.isProcTime(timeOrderFieldType)) {
            throw new TableException(
                    "You must specify either rowtime or proctime for order by as the first one.");
        }

        // time ordering needs to be ascending
        if (!orderKeys.getAscendingOrders()[0]) {
            throw new TableException(
                    "Primary sort order of a streaming table must be ascending on time.");
        }
    }

    @Override
    public Transformation<RowData> translateOrder(
            PlannerBase planner,
            Transformation<RowData> inputTransform,
            RowType inputRowType,
            ExecEdge inputEdge,
            ExecNodeConfig config) {
        SortSpec.SortFieldSpec timeOrderField = matchSpec.getOrderKeys().getFieldSpec(0);
        int timeOrderFieldIdx = timeOrderField.getFieldIndex();
        LogicalType timeOrderFieldType = inputRowType.getTypeAt(timeOrderFieldIdx);

        if (TypeCheckUtils.isRowTime(timeOrderFieldType)) {
            // copy the rowtime field into the StreamRecord timestamp field
            int precision = getPrecision(timeOrderFieldType);
            Transformation<RowData> transform =
                    ExecNodeUtil.createOneInputTransformation(
                            inputTransform,
                            createTransformationMeta(
                                    TIMESTAMP_INSERTER_TRANSFORMATION,
                                    String.format(
                                            "StreamRecordTimestampInserter(rowtime field: %s)",
                                            timeOrderFieldIdx),
                                    "StreamRecordTimestampInserter",
                                    config),
                            new StreamRecordTimestampInserter(timeOrderFieldIdx, precision),
                            inputTransform.getOutputType(),
                            inputTransform.getParallelism(),
                            false);
            if (inputsContainSingleton()) {
                transform.setParallelism(1);
                transform.setMaxParallelism(1);
            }
            return transform;
        } else {
            return inputTransform;
        }
    }
}
