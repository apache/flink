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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Reference to entity local to a certain {@link QueryOperation}. That entity does not come from any
 * of the Operations input. It might be for example a group window in window aggregation.
 */
@Internal
public class LocalReferenceExpression implements ResolvedExpression {

    private final String name;

    private final DataType dataType;

    LocalReferenceExpression(String name, DataType dataType) {
        this.name = Preconditions.checkNotNull(name);
        this.dataType = Preconditions.checkNotNull(dataType);
    }

    public String getName() {
        return name;
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
        LocalReferenceExpression that = (LocalReferenceExpression) o;
        return name.equals(that.name) && dataType.equals(that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
