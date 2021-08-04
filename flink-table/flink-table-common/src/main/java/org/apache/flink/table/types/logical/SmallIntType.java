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

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Logical type of a 2-byte signed integer with values from -32,768 to 32,767.
 *
 * <p>The serialized string representation is {@code SMALLINT}.
 */
@PublicEvolving
public final class SmallIntType extends LogicalType {
    private static final long serialVersionUID = 1L;

    public static final int PRECISION = 5;

    private static final String FORMAT = "SMALLINT";

    private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(Short.class.getName());

    private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION =
            conversionSet(Short.class.getName(), short.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = Short.class;

    public SmallIntType(boolean isNullable) {
        super(isNullable, LogicalTypeRoot.SMALLINT);
    }

    public SmallIntType() {
        this(true);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new SmallIntType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        if (isNullable()) {
            return NULL_OUTPUT_CONVERSION.contains(clazz.getName());
        }
        return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public Class<?> getDefaultConversion() {
        return DEFAULT_CONVERSION;
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
