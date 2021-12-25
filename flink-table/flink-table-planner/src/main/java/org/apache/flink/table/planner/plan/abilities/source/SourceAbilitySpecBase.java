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

package org.apache.flink.table.planner.plan.abilities.source;

import org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonDeserializer;
import org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

import java.util.Optional;

/** Base class for {@link SourceAbilitySpec}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class SourceAbilitySpecBase implements SourceAbilitySpec {

    public static final String FIELD_NAME_PRODUCED_TYPE = "producedType";

    @JsonProperty(FIELD_NAME_PRODUCED_TYPE)
    @JsonSerialize(using = LogicalTypeJsonSerializer.class)
    @JsonDeserialize(using = LogicalTypeJsonDeserializer.class)
    private final @Nullable RowType producedType;

    public SourceAbilitySpecBase() {
        this(null);
    }

    public SourceAbilitySpecBase(@Nullable RowType producedType) {
        this.producedType = producedType;
    }

    @JsonIgnore
    @Override
    public Optional<RowType> getProducedType() {
        return Optional.ofNullable(producedType);
    }
}
