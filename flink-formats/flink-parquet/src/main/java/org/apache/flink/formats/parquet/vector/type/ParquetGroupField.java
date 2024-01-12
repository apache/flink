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

package org.apache.flink.formats.parquet.vector.type;

import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.guava32.com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/** Field that represent parquet's Group Field. */
public class ParquetGroupField extends ParquetField {

    private final List<ParquetField> children;

    public ParquetGroupField(
            LogicalType type,
            int repetitionLevel,
            int definitionLevel,
            boolean required,
            List<ParquetField> children) {
        super(type, repetitionLevel, definitionLevel, required);
        this.children = ImmutableList.copyOf(requireNonNull(children, "children is null"));
    }

    public List<ParquetField> getChildren() {
        return children;
    }
}
