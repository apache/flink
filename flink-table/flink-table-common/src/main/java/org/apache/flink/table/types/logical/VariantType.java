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
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.flink.types.variant.Variant;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Data type of semi-structured data.
 *
 * <p>The type supports storing any semi-structured data, including ARRAY, MAP, and scalar types.
 * VARIANT can only store MAP types with keys of type STRING. The data type of the fields are stored
 * in the data structure, which is close to the semantics of JSON. Compared to ROW and STRUCTURED
 * type, VARIANT type has the flexibility that supports highly nested and evolving schema.
 *
 * <p>The serializable string representation of this type is {@code VARIANT}.
 */
@PublicEvolving
public final class VariantType extends LogicalType {

    private static final Set<String> INPUT_OUTPUT_CONVERSION =
            conversionSet(Variant.class.getName(), BinaryVariant.class.getName());

    public VariantType(boolean isNullable) {
        super(isNullable, LogicalTypeRoot.VARIANT);
    }

    public VariantType() {
        this(true);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new VariantType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability("VARIANT");
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public Class<?> getDefaultConversion() {
        return Variant.class;
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
