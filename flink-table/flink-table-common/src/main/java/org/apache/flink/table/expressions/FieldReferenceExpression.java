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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A reference to a field in an input. The reference contains:
 *
 * <ul>
 *   <li>type
 *   <li>index of an input the field belongs to
 *   <li>index of a field within the corresponding input
 *   <li>optional: alias of the input, if it needs to be referenced by name
 * </ul>
 */
@PublicEvolving
public final class FieldReferenceExpression implements ResolvedExpression {

    private final String name;

    private final DataType dataType;

    /**
     * index of an input the field belongs to. e.g. for a join, `inputIndex` of left input is 0 and
     * `inputIndex` of right input is 1.
     */
    private final int inputIndex;

    /** index of a field within the corresponding input. */
    private final int fieldIndex;

    private final @Nullable String inputAlias;

    public FieldReferenceExpression(
            String name, DataType dataType, int inputIndex, int fieldIndex) {
        this(name, dataType, inputIndex, fieldIndex, null);
    }

    public FieldReferenceExpression(
            String name,
            DataType dataType,
            int inputIndex,
            int fieldIndex,
            @Nullable String inputAlias) {
        Preconditions.checkArgument(inputIndex >= 0, "Index of input should be a positive number");
        Preconditions.checkArgument(fieldIndex >= 0, "Index of field should be a positive number");
        this.name = Preconditions.checkNotNull(name, "Field name must not be null.");
        this.dataType = Preconditions.checkNotNull(dataType, "Field data type must not be null.");
        this.inputIndex = inputIndex;
        this.fieldIndex = fieldIndex;
        this.inputAlias = inputAlias;
    }

    public String getName() {
        return name;
    }

    public int getInputIndex() {
        return inputIndex;
    }

    public int getFieldIndex() {
        return fieldIndex;
    }

    @Override
    public DataType getOutputDataType() {
        return dataType;
    }

    @Override
    public List<ResolvedExpression> getResolvedChildren() {
        return Collections.emptyList();
    }

    @Override
    public String asSummaryString() {
        return name;
    }

    @Override
    public String asSerializableString() {
        if (inputAlias != null) {
            return String.format(
                    "%s.%s",
                    EncodingUtils.escapeIdentifier(inputAlias),
                    EncodingUtils.escapeIdentifier(name));
        }
        return EncodingUtils.escapeIdentifier(name);
    }

    @Override
    public List<Expression> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
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
        FieldReferenceExpression that = (FieldReferenceExpression) o;
        return name.equals(that.name)
                && dataType.equals(that.dataType)
                && inputIndex == that.inputIndex
                && fieldIndex == that.fieldIndex
                && Objects.equals(inputAlias, that.inputAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, inputIndex, fieldIndex, inputAlias);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
