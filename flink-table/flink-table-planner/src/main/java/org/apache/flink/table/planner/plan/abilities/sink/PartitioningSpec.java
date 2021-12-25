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
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A sub-class of {@link SinkAbilitySpec} that can not only serialize/deserialize the partition
 * to/from JSON, but also can write partitioned data for {@link SupportsWritingMetadata}.
 */
@JsonTypeName("Partitioning")
public class PartitioningSpec implements SinkAbilitySpec {
    public static final String FIELD_NAME_PARTITION = "partition";

    @JsonProperty(FIELD_NAME_PARTITION)
    private final Map<String, String> partition;

    @JsonCreator
    public PartitioningSpec(@JsonProperty(FIELD_NAME_PARTITION) Map<String, String> partition) {
        this.partition = checkNotNull(partition);
    }

    @Override
    public void apply(DynamicTableSink tableSink) {
        if (tableSink instanceof SupportsPartitioning) {
            ((SupportsPartitioning) tableSink).applyStaticPartition(partition);
        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsPartitioning.",
                            tableSink.getClass().getName()));
        }
    }
}
