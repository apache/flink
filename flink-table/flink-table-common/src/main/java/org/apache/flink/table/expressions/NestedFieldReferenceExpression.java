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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A reference to a nested field in an input. The reference contains.
 *
 * <ul>
 *   <li>type
 *   <li>index of an input the field belongs to
 *   <li>index of a field within the corresponding input
 * </ul>
 */
@PublicEvolving
public class NestedFieldReferenceExpression implements ResolvedExpression {

    /** Nested field names to traverse from the top level column to the nested leaf column. */
    private final String[] fieldNames;

    private final DataType dataType;

    public NestedFieldReferenceExpression(String[] fieldNames, DataType dataType) {
        this.fieldNames = fieldNames;
        this.dataType = dataType;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public String getName() {
        return String.join("_", fieldNames);
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
        return getName();
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
        NestedFieldReferenceExpression that = (NestedFieldReferenceExpression) o;
        return Arrays.equals(fieldNames, that.fieldNames) && dataType.equals(that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldNames, dataType);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
