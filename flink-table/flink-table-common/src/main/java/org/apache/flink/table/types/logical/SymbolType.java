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

import java.util.Collections;
import java.util.List;

/**
 * Logical type for representing symbol values. The symbol type is an extension to the SQL standard
 * and only serves as a helper type within the expression stack.
 *
 * <p>A symbol type accepts conversions from and to {@link Enum}. But note that this is not an enum
 * type for users.
 *
 * <p>This type has no serializable string representation.
 *
 * @param <T> Legacy generic that will be dropped in the next major version. If we dropped it
 *     earlier, we would break {@link LogicalTypeVisitor} implementation.
 */
@PublicEvolving
public final class SymbolType<T extends TableSymbol> extends LogicalType {
    private static final long serialVersionUID = 2L;

    private static final String FORMAT = "SYMBOL";

    /** @deprecated Symbol types have been simplified to not require a class. */
    @Deprecated
    public SymbolType(boolean isNullable, @SuppressWarnings("unused") Class<T> clazz) {
        this(isNullable);
    }

    /** @deprecated Symbol types have been simplified to not require a class. */
    @Deprecated
    public SymbolType(@SuppressWarnings("unused") Class<T> clazz) {
        this();
    }

    public SymbolType(boolean isNullable) {
        super(isNullable, LogicalTypeRoot.SYMBOL);
    }

    public SymbolType() {
        this(true);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new SymbolType<>(isNullable);
    }

    @Override
    public String asSummaryString() {
        return withNullability(FORMAT);
    }

    @Override
    public String asSerializableString() {
        throw new TableException("A symbol type has no serializable string representation.");
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return Enum.class.isAssignableFrom(clazz);
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return Enum.class.isAssignableFrom(clazz);
    }

    @Override
    public Class<?> getDefaultConversion() {
        return Enum.class;
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
