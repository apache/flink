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
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * A sub-class of {@link SinkAbilitySpec} that can not only serialize/deserialize the overwrite flag
 * to/from JSON, but also can overwrite existing data for {@link SupportsOverwrite}.
 */
@JsonTypeName("Overwrite")
public class OverwriteSpec implements SinkAbilitySpec {
    public static final String FIELD_NAME_OVERWRITE = "overwrite";

    @JsonProperty(FIELD_NAME_OVERWRITE)
    private final boolean overwrite;

    @JsonCreator
    public OverwriteSpec(@JsonProperty(FIELD_NAME_OVERWRITE) boolean overwrite) {
        this.overwrite = overwrite;
    }

    @Override
    public void apply(DynamicTableSink tableSink) {
        if (tableSink instanceof SupportsOverwrite) {
            ((SupportsOverwrite) tableSink).applyOverwrite(overwrite);
        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsOverwrite.",
                            tableSink.getClass().getName()));
        }
    }
}
