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

import org.apache.parquet.column.ColumnDescriptor;

import static java.util.Objects.requireNonNull;

/** Field that represent parquet's primitive field. */
public class ParquetPrimitiveField extends ParquetField {

    private final ColumnDescriptor descriptor;
    private final int id;

    public ParquetPrimitiveField(
            LogicalType type, boolean required, ColumnDescriptor descriptor, int id) {
        super(
                type,
                descriptor.getMaxRepetitionLevel(),
                descriptor.getMaxDefinitionLevel(),
                required);
        this.descriptor = requireNonNull(descriptor, "descriptor is required");
        this.id = id;
    }

    public ColumnDescriptor getDescriptor() {
        return descriptor;
    }

    public int getId() {
        return id;
    }
}
