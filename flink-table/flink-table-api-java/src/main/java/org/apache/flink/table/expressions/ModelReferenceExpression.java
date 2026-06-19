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
import org.apache.flink.table.api.Model;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A reference to a {@link Model} in an expression context.
 *
 * <p>This expression is used when a model needs to be passed as an argument to functions or
 * operations that accept model references. It wraps a model object and provides the necessary
 * expression interface for use in the Table API expression system.
 *
 * <p>The expression carries a string representation of the model and uses a special data type to
 * indicate that this is a model reference rather than a regular data value.
 */
@Internal
public final class ModelReferenceExpression implements ResolvedExpression {

    private final String name;
    private final ContextResolvedModel model;
    // The environment is optional but serves validation purposes
    // to ensure that all referenced tables belong to the same
    // environment.
    private final TableEnvironment env;

    public ModelReferenceExpression(String name, ContextResolvedModel model, TableEnvironment env) {
        this.name = Preconditions.checkNotNull(name);
        this.model = Preconditions.checkNotNull(model);
        this.env = Preconditions.checkNotNull(env);
    }

    /**
     * Returns the name of this model reference.
     *
     * @return the model reference name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the ContextResolvedModel associated with this model reference.
     *
     * @return the query context resolved model
     */
    public ContextResolvedModel getModel() {
        return model;
    }

    public @Nullable TableEnvironment getTableEnvironment() {
        return env;
    }

    /**
     * Returns the input data type expected by this model reference.
     *
     * <p>This method extracts the input data type from the model's input schema, which describes
     * the structure and data types that the model expects for inference operations.
     *
     * @return the input data type expected by the model
     */
    public DataType getInputDataType() {
        return model.getResolvedModel().getResolvedInputSchema().toPhysicalRowDataType();
    }

    @Override
    public DataType getOutputDataType() {
        return model.getResolvedModel().getResolvedOutputSchema().toPhysicalRowDataType();
    }

    @Override
    public List<ResolvedExpression> getResolvedChildren() {
        return Collections.emptyList();
    }

    @Override
    public String asSerializableString(SqlFactory sqlFactory) {
        if (model.isAnonymous()) {
            throw new ValidationException("Anonymous models cannot be serialized.");
        }

        return "MODEL " + model.getIdentifier().asSerializableString();
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
        ModelReferenceExpression that = (ModelReferenceExpression) o;
        return Objects.equals(name, that.name)
                && Objects.equals(model, that.model)
                // Effectively means reference equality
                && Objects.equals(env, that.env);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, model, env);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
