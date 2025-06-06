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
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.ColumnList;

import java.util.Collections;
import java.util.List;

/**
 * Logical type for describing an arbitrary, unvalidated list of columns.
 *
 * <p>This type is the return type of calls to {@code DESCRIPTOR(`c0` INT, `c1` STRING)} (not
 * supported by Calcite yet) or {@code DESCRIPTOR(`c0`, `c1`)}. The type is intended to be used in
 * arguments of {@link ProcessTableFunction}s.
 *
 * <p>Note: The runtime does not support this type. It is a pure helper type during translation and
 * planning. Table columns cannot be declared with this type. Functions cannot declare return types
 * of this type.
 *
 * <p>The serialized string representation is {@code DESCRIPTOR}.
 */
@PublicEvolving
public final class DescriptorType extends LogicalType {
    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "DESCRIPTOR";

    private static final Class<?> INPUT_OUTPUT_CONVERSION = ColumnList.class;

    private static final Class<?> DEFAULT_CONVERSION = ColumnList.class;

    public DescriptorType(boolean isNullable) {
        super(isNullable, LogicalTypeRoot.DESCRIPTOR);
    }

    public DescriptorType() {
        this(true);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new DescriptorType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION == clazz;
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION == clazz;
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
