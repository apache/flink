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

package org.apache.flink.table.gateway.api.results;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/** A column info represents a table column's structure with column name, column type. */
@PublicEvolving
public class ColumnInfo {

    private static final String FIELD_NAME_NAME = "name";
    private static final String FIELD_NAME_TYPE = "logicalType";

    @JsonProperty(FIELD_NAME_NAME)
    private final String name;

    @JsonProperty(FIELD_NAME_TYPE)
    private final LogicalType logicalType;

    @JsonCreator
    public ColumnInfo(
            @JsonProperty(FIELD_NAME_NAME) String name,
            @JsonProperty(FIELD_NAME_TYPE) LogicalType logicalType) {
        this.name = Preconditions.checkNotNull(name, "name must not be null");
        this.logicalType = Preconditions.checkNotNull(logicalType, "logical type must not be null");
    }

    public static ColumnInfo create(String name, LogicalType type) {
        return new ColumnInfo(name, type);
    }

    public String getName() {
        return name;
    }

    public LogicalType getLogicalType() {
        return logicalType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnInfo that = (ColumnInfo) o;
        return name.equals(that.name) && logicalType.equals(that.logicalType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, logicalType);
    }

    @Override
    public String toString() {
        return String.format(
                "ColumnInfo{name='%s', logical type='%s'}", name, logicalType.toString());
    }
}
