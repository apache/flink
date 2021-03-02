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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Optional;

/**
 * An interface that can not only serialize/deserialize the source abilities to/from JSON, but also
 * can apply the abilities to a {@link DynamicTableSource}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = FilterPushDownSpec.class),
    @JsonSubTypes.Type(value = LimitPushDownSpec.class),
    @JsonSubTypes.Type(value = PartitionPushDownSpec.class),
    @JsonSubTypes.Type(value = ProjectPushDownSpec.class),
    @JsonSubTypes.Type(value = ReadingMetadataSpec.class),
    @JsonSubTypes.Type(value = WatermarkPushDownSpec.class)
})
@Internal
public interface SourceAbilitySpec {

    /** Apply the ability to the given {@link DynamicTableSource}. */
    void apply(DynamicTableSource tableSource, SourceAbilityContext context);

    /**
     * Return the produced {@link RowType} this the ability is applied.
     *
     * <p>NOTE: If the ability does not change the produced type, this method will return {@link
     * Optional#empty}.
     */
    @JsonIgnore
    Optional<RowType> getProducedType();
}
