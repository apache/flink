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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A sub-class of {@link SourceAbilitySpec} that can not only serialize/deserialize the projection
 * to/from JSON, but also can push the projection into a {@link SupportsProjectionPushDown}.
 */
@JsonTypeName("ProjectPushDown")
public final class ProjectPushDownSpec extends SourceAbilitySpecBase {
    public static final String FIELD_NAME_PROJECTED_FIELDS = "projectedFields";

    @JsonProperty(FIELD_NAME_PROJECTED_FIELDS)
    private final int[][] projectedFields;

    @JsonCreator
    public ProjectPushDownSpec(
            @JsonProperty(FIELD_NAME_PROJECTED_FIELDS) int[][] projectedFields,
            @JsonProperty(FIELD_NAME_PRODUCED_TYPE) RowType producedType) {
        super(producedType);
        this.projectedFields = checkNotNull(projectedFields);
    }

    @Override
    public void apply(DynamicTableSource tableSource, SourceAbilityContext context) {
        if (tableSource instanceof SupportsProjectionPushDown) {
            ((SupportsProjectionPushDown) tableSource)
                    .applyProjection(projectedFields, DataTypes.of(getProducedType().get()));
        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsProjectionPushDown.",
                            tableSource.getClass().getName()));
        }
    }

    @Override
    public boolean needAdjustFieldReferenceAfterProjection() {
        return false;
    }

    public int[][] getProjectedFields() {
        return projectedFields;
    }

    @Override
    public String getDigests(SourceAbilityContext context) {
        final List<String> fieldNames =
                this.getProducedType()
                        .orElseThrow(() -> new TableException("Produced data type is not present."))
                        .getFieldNames();

        return String.format("project=[%s]", String.join(", ", fieldNames));
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
        ProjectPushDownSpec that = (ProjectPushDownSpec) o;
        return Arrays.deepEquals(projectedFields, that.projectedFields);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.deepHashCode(projectedFields);
        return result;
    }
}
