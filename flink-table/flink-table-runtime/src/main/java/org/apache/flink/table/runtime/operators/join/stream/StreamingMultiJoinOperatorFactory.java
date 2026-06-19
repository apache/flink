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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.MultiJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Serializable factory for creating {@link StreamingMultiJoinOperator}. */
public class StreamingMultiJoinOperatorFactory extends AbstractStreamOperatorFactory<RowData>
        implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<InternalTypeInfo<RowData>> inputTypeInfos;
    private final List<JoinInputSideSpec> inputSideSpecs;
    private final List<FlinkJoinType> joinTypes;
    private final MultiJoinCondition multiJoinCondition;
    private final long[] stateRetentionTime;
    private final GeneratedJoinCondition[] joinConditions;
    private final JoinKeyExtractor keyExtractor;
    private final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap;

    public StreamingMultiJoinOperatorFactory(
            final List<InternalTypeInfo<RowData>> inputTypeInfos,
            final List<JoinInputSideSpec> inputSideSpecs,
            final List<FlinkJoinType> joinTypes,
            @Nullable final MultiJoinCondition multiJoinCondition,
            final long[] stateRetentionTime,
            final GeneratedJoinCondition[] joinConditions,
            final JoinKeyExtractor keyExtractor,
            final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap) {
        this.inputTypeInfos = inputTypeInfos;
        this.inputSideSpecs = inputSideSpecs;
        this.joinTypes = joinTypes;
        this.multiJoinCondition = multiJoinCondition;
        this.stateRetentionTime = stateRetentionTime;
        this.joinConditions = joinConditions;
        this.keyExtractor = keyExtractor;
        this.joinAttributeMap = joinAttributeMap;
    }

    @Override
    public <T extends StreamOperator<RowData>> T createStreamOperator(
            final StreamOperatorParameters<RowData> parameters) {
        final var inputRowTypes =
                inputTypeInfos.stream()
                        .map(InternalTypeInfo::toRowType)
                        .collect(Collectors.toList());

        final StreamingMultiJoinOperator operator =
                new StreamingMultiJoinOperator(
                        parameters,
                        inputRowTypes,
                        inputSideSpecs,
                        joinTypes,
                        multiJoinCondition,
                        stateRetentionTime,
                        joinConditions,
                        keyExtractor,
                        joinAttributeMap);

        @SuppressWarnings("unchecked")
        final T castedOperator = (T) operator;
        return castedOperator;
    }

    @Override
    public Class<? extends StreamOperator<RowData>> getStreamOperatorClass(
            final ClassLoader classLoader) {
        return StreamingMultiJoinOperator.class;
    }
}
