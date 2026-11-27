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
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.misc.ChangelogFilterFunction;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link ExecNode} that keeps only the rows whose {@link RowKind} is in a given set and
 * drops all others. The operator keeps no state.
 *
 * <p>The planner uses it to drop UPDATE_BEFORE and DELETE rows below a function that ignores
 * retractions (see {@link StaticArgumentTrait#IGNORE_RETRACTIONS}).
 */
@ExecNodeMetadata(
        name = "stream-exec-changelog-filter",
        version = 1,
        producedTransformations = StreamExecChangelogFilter.CHANGELOG_FILTER_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_5,
        minStateVersion = FlinkVersion.v2_5)
public class StreamExecChangelogFilter extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String CHANGELOG_FILTER_TRANSFORMATION = "changelog-filter";

    public static final String FIELD_NAME_KEPT_ROW_KINDS = "keptRowKinds";

    @JsonProperty(FIELD_NAME_KEPT_ROW_KINDS)
    private final List<RowKind> keptRowKinds;

    public StreamExecChangelogFilter(
            ReadableConfig tableConfig,
            List<RowKind> keptRowKinds,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecChangelogFilter.class),
                ExecNodeContext.newPersistedConfig(StreamExecChangelogFilter.class, tableConfig),
                keptRowKinds,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecChangelogFilter(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_KEPT_ROW_KINDS) List<RowKind> keptRowKinds,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.keptRowKinds = checkNotNull(keptRowKinds);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
        final EnumSet<RowKind> kept = EnumSet.noneOf(RowKind.class);
        kept.addAll(keptRowKinds);
        final StreamFilter<RowData> operator =
                new StreamFilter<>(new ChangelogFilterFunction(kept));

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(CHANGELOG_FILTER_TRANSFORMATION, config),
                operator,
                inputTransform.getOutputType(),
                inputTransform.getParallelism(),
                false);
    }
}
