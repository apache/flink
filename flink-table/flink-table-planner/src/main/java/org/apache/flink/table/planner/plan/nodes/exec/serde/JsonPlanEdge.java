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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * The {@link ExecEdge}'s JSON representation.
 *
 * <p>Differs from {@link ExecEdge}, since {@link JsonPlanEdge} only stores the {@link
 * ExecNode#getId()} instead of instance.
 *
 * <p>This model is used only during serialization/deserialization.
 */
@Internal
@JsonIgnoreProperties(ignoreUnknown = true)
final class JsonPlanEdge {
    static final String FIELD_NAME_SOURCE = "source";
    static final String FIELD_NAME_TARGET = "target";
    static final String FIELD_NAME_SHUFFLE = "shuffle";
    static final String FIELD_NAME_SHUFFLE_MODE = "shuffleMode";

    /** The source node id of this edge. */
    @JsonProperty(FIELD_NAME_SOURCE)
    private final int sourceId;
    /** The target node id of this edge. */
    @JsonProperty(FIELD_NAME_TARGET)
    private final int targetId;
    /** The {@link ExecEdge.Shuffle} on this edge from source to target. */
    @JsonProperty(FIELD_NAME_SHUFFLE)
    @JsonSerialize(using = ShuffleJsonSerializer.class)
    @JsonDeserialize(using = ShuffleJsonDeserializer.class)
    private final ExecEdge.Shuffle shuffle;
    /** The {@link StreamExchangeMode} on this edge. */
    @JsonProperty(FIELD_NAME_SHUFFLE_MODE)
    private final StreamExchangeMode exchangeMode;

    @JsonCreator
    JsonPlanEdge(
            @JsonProperty(FIELD_NAME_SOURCE) int sourceId,
            @JsonProperty(FIELD_NAME_TARGET) int targetId,
            @JsonProperty(FIELD_NAME_SHUFFLE) ExecEdge.Shuffle shuffle,
            @JsonProperty(FIELD_NAME_SHUFFLE_MODE) StreamExchangeMode exchangeMode) {
        this.sourceId = sourceId;
        this.targetId = targetId;
        this.shuffle = shuffle;
        this.exchangeMode = exchangeMode;
    }

    int getSourceId() {
        return sourceId;
    }

    int getTargetId() {
        return targetId;
    }

    ExecEdge.Shuffle getShuffle() {
        return shuffle;
    }

    StreamExchangeMode getExchangeMode() {
        return exchangeMode;
    }

    /** Build {@link JsonPlanEdge} from an {@link ExecEdge}. */
    static JsonPlanEdge fromExecEdge(ExecEdge execEdge) {
        return new JsonPlanEdge(
                execEdge.getSource().getId(),
                execEdge.getTarget().getId(),
                execEdge.getShuffle(),
                execEdge.getExchangeMode());
    }
}
