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
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

import java.util.Objects;

/** A column info represents a table column's structure with column name, column type. */
@Internal
public class ColumnInfo {

    private static final String FIELD_NAME_NAME = "name";
    private static final String FIELD_NAME_TYPE = "logicalType";
    private static final String FIELD_NAME_COMMENT = "comment";

    @JsonProperty(FIELD_NAME_NAME)
    private final String name;

    @JsonProperty(FIELD_NAME_TYPE)
    @JsonSerialize(using = LogicalTypeJsonSerializer.class)
    @JsonDeserialize(using = LogicalTypeJsonDeserializer.class)
    private final LogicalType logicalType;

    @JsonProperty(FIELD_NAME_COMMENT)
    private @Nullable final String comment;

    @JsonCreator
    public ColumnInfo(
            @JsonProperty(FIELD_NAME_NAME) String name,
            @JsonProperty(FIELD_NAME_TYPE) LogicalType logicalType,
            @JsonProperty(FIELD_NAME_COMMENT) String comment) {
        this.name = Preconditions.checkNotNull(name, "name must not be null");
        this.logicalType = Preconditions.checkNotNull(logicalType, "logical type must not be null");
        this.comment = comment;
    }

    public static ColumnInfo toColumnInfo(Column column) {
        return new ColumnInfo(
                column.getName(),
                column.getDataType().getLogicalType(),
                column.getComment().orElse(null));
    }

    public Column toColumn() {
        return Column.physical(name, DataTypeUtils.toInternalDataType(logicalType))
                .withComment(comment);
    }

    public String getName() {
        return name;
    }

    public LogicalType getLogicalType() {
        return logicalType;
    }

    public @Nullable String getComment() {
        return comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnInfo)) {
            return false;
        }
        ColumnInfo that = (ColumnInfo) o;
        return Objects.equals(name, that.name)
                && Objects.equals(logicalType, that.logicalType)
                && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, logicalType, comment);
    }

    @Override
    public String toString() {
        return "ColumnInfo{"
                + "name='"
                + name
                + '\''
                + ", logicalType="
                + logicalType
                + ", comment='"
                + comment
                + '\''
                + '}';
    }
}
