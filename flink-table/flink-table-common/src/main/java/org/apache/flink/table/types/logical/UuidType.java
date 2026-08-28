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

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Data type of a universally unique identifier (UUID).
 *
 * <p>The type stores any 128-bit UUID as the canonical 16-byte big-endian encoding defined by RFC
 * 9562 (which obsoletes RFC 4122). It is version-agnostic: the version and variant bits are stored
 * as-is and never validated or interpreted.
 *
 * <p>The serializable string representation of this type is {@code UUID}.
 */
@PublicEvolving
public final class UuidType extends LogicalType {

    private static final long serialVersionUID = 1L;

    private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(UUID.class.getName());

    public UuidType(boolean isNullable) {
        super(isNullable, LogicalTypeRoot.UUID);
    }

    public UuidType() {
        this(true);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new UuidType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability("UUID");
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
        return UUID.class;
    }

    @Override
    public List<LogicalType> getChildren() {
        return List.of();
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
