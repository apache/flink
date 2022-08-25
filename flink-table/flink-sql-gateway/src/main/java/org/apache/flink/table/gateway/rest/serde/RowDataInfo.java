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

package org.apache.flink.table.gateway.rest.serde;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/** A RowDataInfo info represents a {@link RowData}. */
@Internal
public class RowDataInfo {

    private static final String FIELD_NAME_KIND = "kind";
    private static final String FIELD_NAME_FIELDS = "fields";

    @JsonProperty(FIELD_NAME_KIND)
    private final String kind;

    @JsonProperty(FIELD_NAME_FIELDS)
    private final List<JsonNode> fields;

    @JsonCreator
    public RowDataInfo(
            @JsonProperty(FIELD_NAME_KIND) String kind,
            @JsonProperty(FIELD_NAME_FIELDS) List<JsonNode> fields) {
        this.kind = Preconditions.checkNotNull(kind, "kind must not be null");
        this.fields = Preconditions.checkNotNull(fields, "fields must not be null");
    }

    public String getKind() {
        return kind;
    }

    public List<JsonNode> getFields() {
        return fields;
    }
}
