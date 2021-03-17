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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.TableSymbol;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Logical type for representing symbol values. The symbol type is an extension to the SQL standard
 * and only serves as a helper type within the expression stack.
 *
 * <p>A symbol type only accepts conversions from and to its enum class.
 *
 * <p>This type has no serializable string representation.
 *
 * @param <T> table symbol
 */
@PublicEvolving
public final class SymbolType<T extends TableSymbol> extends LogicalType {

    private static final String FORMAT = "SYMBOL('%s')";

    private final Class<T> symbolClass;

    public SymbolType(boolean isNullable, Class<T> symbolClass) {
        super(isNullable, LogicalTypeRoot.SYMBOL);
        this.symbolClass =
                Preconditions.checkNotNull(symbolClass, "Symbol class must not be null.");
    }

    public SymbolType(Class<T> symbolClass) {
        this(true, symbolClass);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new SymbolType<>(isNullable, symbolClass);
    }

    @Override
    public String asSummaryString() {
        return withNullability(FORMAT, symbolClass.getName());
    }

    @Override
    public String asSerializableString() {
        throw new TableException("A symbol type has no serializable string representation.");
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return symbolClass.equals(clazz);
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return symbolClass.equals(clazz);
    }

    @Override
    public Class<?> getDefaultConversion() {
        return symbolClass;
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        SymbolType<?> that = (SymbolType<?>) o;
        return symbolClass.equals(that.symbolClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), symbolClass);
    }
}
