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

/** Field that represent parquet's field type. */
public abstract class ParquetField {
    private final LogicalType type;
    private final int repetitionLevel;
    private final int definitionLevel;
    private final boolean required;

    public ParquetField(
            LogicalType type, int repetitionLevel, int definitionLevel, boolean required) {
        this.type = type;
        this.repetitionLevel = repetitionLevel;
        this.definitionLevel = definitionLevel;
        this.required = required;
    }

    public LogicalType getType() {
        return type;
    }

    public int getRepetitionLevel() {
        return repetitionLevel;
    }

    public int getDefinitionLevel() {
        return definitionLevel;
    }

    public boolean isRequired() {
        return required;
    }

    @Override
    public String toString() {
        return "Field{"
                + "type="
                + type
                + ", repetitionLevel="
                + repetitionLevel
                + ", definitionLevel="
                + definitionLevel
                + ", required="
                + required
                + '}';
    }
}
