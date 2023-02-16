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

package org.apache.flink.table.planner.plan.abilities.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.sink.DynamicTableSink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * An interface that can not only serialize/deserialize the sink abilities to/from JSON, but also
 * can apply the abilities to a {@link DynamicTableSink}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = OverwriteSpec.class),
    @JsonSubTypes.Type(value = PartitioningSpec.class),
    @JsonSubTypes.Type(value = WritingMetadataSpec.class),
    @JsonSubTypes.Type(value = RowLevelDeleteSpec.class),
    @JsonSubTypes.Type(value = RowLevelUpdateSpec.class)
})
@Internal
public interface SinkAbilitySpec {

    /** Apply the ability to the given {@link DynamicTableSink}. */
    void apply(DynamicTableSink tableSink);
}
