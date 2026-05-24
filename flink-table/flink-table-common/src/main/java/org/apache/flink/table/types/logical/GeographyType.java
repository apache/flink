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

package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.data.GeographyData;

import java.util.Collections;
import java.util.List;

/**
 * Data type of geography data.
 *
 * <p>The serializable string representation of this type is {@code GEOGRAPHY}.
 */
@PublicEvolving
public final class GeographyType extends LogicalType {

    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "GEOGRAPHY";

    private static final Class<?> INPUT_OUTPUT_CONVERSION = GeographyData.class;

    public GeographyType(boolean isNullable) {
        super(isNullable, LogicalTypeRoot.GEOGRAPHY);
    }

    public GeographyType() {
        this(true);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new GeographyType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.isAssignableFrom(clazz);
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.isAssignableFrom(clazz);
    }

    @Override
    public Class<?> getDefaultConversion() {
        return INPUT_OUTPUT_CONVERSION;
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
