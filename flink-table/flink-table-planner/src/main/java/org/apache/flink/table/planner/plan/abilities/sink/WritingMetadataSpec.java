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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonDeserializer;
import org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;

/**
 * A sub-class of {@link SinkAbilitySpec} that can not only serialize/deserialize the metadata
 * columns to/from JSON, but also can write the metadata columns for {@link
 * SupportsWritingMetadata}.
 */
@JsonTypeName("WritingMetadata")
public class WritingMetadataSpec implements SinkAbilitySpec {
    public static final String FIELD_NAME_METADATA_KEYS = "metadataKeys";
    public static final String FIELD_NAME_CONSUMED_TYPE = "consumedType";

    @JsonProperty(FIELD_NAME_METADATA_KEYS)
    private final List<String> metadataKeys;

    @JsonProperty(FIELD_NAME_CONSUMED_TYPE)
    @JsonSerialize(using = LogicalTypeJsonSerializer.class)
    @JsonDeserialize(using = LogicalTypeJsonDeserializer.class)
    private final LogicalType consumedType;

    @JsonCreator
    public WritingMetadataSpec(
            @JsonProperty(FIELD_NAME_METADATA_KEYS) List<String> metadataKeys,
            @JsonProperty(FIELD_NAME_CONSUMED_TYPE) LogicalType consumedType) {
        this.metadataKeys = metadataKeys;
        this.consumedType = consumedType;
    }

    @Override
    public void apply(DynamicTableSink tableSink) {
        if (tableSink instanceof SupportsWritingMetadata) {
            DataType consumedDataType = TypeConversions.fromLogicalToDataType(consumedType);
            ((SupportsWritingMetadata) tableSink)
                    .applyWritableMetadata(metadataKeys, consumedDataType);
        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsWritingMetadata.",
                            tableSink.getClass().getName()));
        }
    }
}
