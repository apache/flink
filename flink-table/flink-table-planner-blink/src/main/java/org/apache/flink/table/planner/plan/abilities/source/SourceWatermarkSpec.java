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
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * A sub-class of {@link SourceAbilitySpec} that can not only serialize/deserialize the boolean flag
 * whether a source watermark should be used to/from JSON, but can also apply it to {@link
 * SupportsSourceWatermark}.
 */
@JsonTypeName("SourceWatermark")
public class SourceWatermarkSpec extends SourceAbilitySpecBase {
    public static final String FIELD_NAME_SOURCE_WATERMARK_ENABLED = "sourceWatermarkEnabled";

    @JsonProperty(FIELD_NAME_SOURCE_WATERMARK_ENABLED)
    private final boolean sourceWatermarkEnabled;

    @JsonCreator
    public SourceWatermarkSpec(
            @JsonProperty(FIELD_NAME_SOURCE_WATERMARK_ENABLED) boolean sourceWatermarkEnabled,
            @JsonProperty(FIELD_NAME_PRODUCED_TYPE) RowType producedType) {
        super(producedType);
        this.sourceWatermarkEnabled = sourceWatermarkEnabled;
    }

    @Override
    public void apply(DynamicTableSource tableSource, SourceAbilityContext context) {
        if (tableSource instanceof SupportsSourceWatermark) {
            if (sourceWatermarkEnabled) {
                ((SupportsSourceWatermark) tableSource).applySourceWatermark();
            }
        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsSourceWatermark.",
                            tableSource.getClass().getName()));
        }
    }
}
