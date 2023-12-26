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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A sub-class of {@link SourceAbilitySpec} that can not only serialize/deserialize the partitions
 * to/from JSON, but also can push the partitions into a {@link SupportsPartitionPushDown}.
 */
@JsonTypeName("PartitionPushDown")
public final class PartitionPushDownSpec extends SourceAbilitySpecBase {
    public static final String FIELD_NAME_PARTITIONS = "partitions";

    @JsonProperty(FIELD_NAME_PARTITIONS)
    private final List<Map<String, String>> partitions;

    @JsonCreator
    public PartitionPushDownSpec(
            @JsonProperty(FIELD_NAME_PARTITIONS) List<Map<String, String>> partitions) {
        this.partitions = new ArrayList<>(checkNotNull(partitions));
    }

    @Override
    public void apply(DynamicTableSource tableSource, SourceAbilityContext context) {
        if (tableSource instanceof SupportsPartitionPushDown) {
            ((SupportsPartitionPushDown) tableSource).applyPartitions(partitions);
        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsPartitionPushDown.",
                            tableSource.getClass().getName()));
        }
    }

    public List<Map<String, String>> getPartitions() {
        return partitions;
    }

    @Override
    public boolean needAdjustFieldReferenceAfterProjection() {
        return false;
    }

    @Override
    public String getDigests(SourceAbilityContext context) {
        return "partitions=["
                + this.partitions.stream().map(Object::toString).collect(Collectors.joining(", "))
                + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PartitionPushDownSpec that = (PartitionPushDownSpec) o;
        return Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), partitions);
    }
}
