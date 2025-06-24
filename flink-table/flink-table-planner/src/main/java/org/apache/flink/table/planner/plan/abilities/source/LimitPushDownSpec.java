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
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

/**
 * A sub-class of {@link SourceAbilitySpec} that can not only serialize/deserialize the limit value
 * to/from JSON, but also can push the limit value into a {@link LimitPushDownSpec}.
 */
@JsonTypeName("LimitPushDown")
public final class LimitPushDownSpec extends SourceAbilitySpecBase {
    public static final String FIELD_NAME_LIMIT = "limit";

    @JsonProperty(FIELD_NAME_LIMIT)
    private final long limit;

    @JsonCreator
    public LimitPushDownSpec(@JsonProperty(FIELD_NAME_LIMIT) long limit) {
        this.limit = limit;
    }

    @Override
    public void apply(DynamicTableSource tableSource, SourceAbilityContext context) {
        if (tableSource instanceof SupportsLimitPushDown) {
            ((SupportsLimitPushDown) tableSource).applyLimit(limit);
        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsLimitPushDown.",
                            tableSource.getClass().getName()));
        }
    }

    @Override
    public boolean needAdjustFieldReferenceAfterProjection() {
        return false;
    }

    @Override
    public String getDigests(SourceAbilityContext context) {
        return "limit=[" + this.limit + "]";
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
        LimitPushDownSpec that = (LimitPushDownSpec) o;
        return limit == that.limit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), limit);
    }
}
