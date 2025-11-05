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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ModelSemantics;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;
import java.util.Optional;

/** Provides details about a function call during {@link TypeInference}. */
@PublicEvolving
public interface CallContext {

    /** Enables to lookup types in a catalog and resolve RAW types. */
    DataTypeFactory getDataTypeFactory();

    /** Returns the function definition that defines the function currently being called. */
    FunctionDefinition getFunctionDefinition();

    /** Returns whether the argument at the given position is a value literal. */
    boolean isArgumentLiteral(int pos);

    /**
     * Returns {@code true} if the argument at the given position is a literal and {@code null},
     * {@code false} otherwise. If the argument is declared as optional and has no value, true is
     * returned.
     *
     * <p>Use {@link #isArgumentLiteral(int)} before to check if the argument is actually a literal.
     */
    boolean isArgumentNull(int pos);

    /**
     * Returns the literal value of the argument at the given position, given that the argument is a
     * literal, is not null, and can be expressed as an instance of the provided class.
     *
     * <p>It supports conversions to default conversion classes of {@link LogicalType LogicalTypes}.
     * This method should not be called with other classes.
     *
     * <p>Use {@link #isArgumentLiteral(int)} before to check if the argument is actually a literal.
     */
    <T> Optional<T> getArgumentValue(int pos, Class<T> clazz);

    /**
     * Returns information about the table that has been passed to a table argument.
     *
     * <p>This method applies only to {@link ProcessTableFunction}s.
     *
     * <p>Semantics are only available for table arguments that are annotated with
     * {@code @ArgumentHint(SET_SEMANTIC_TABLE)} or {@code @ArgumentHint(ROW_SEMANTIC_TABLE)}).
     */
    default Optional<TableSemantics> getTableSemantics(int pos) {
        return Optional.empty();
    }

    /**
     * Returns information about the model that has been passed to a model argument.
     *
     * <p>This method applies only to {@link ProcessTableFunction}s.
     */
    default Optional<ModelSemantics> getModelSemantics(int pos) {
        return Optional.empty();
    }

    /**
     * Returns the {@link ChangelogMode} that the framework requires from the function.
     *
     * <p>This method applies only to {@link ProcessTableFunction}.
     *
     * <p>Returns empty during type inference phase as the changelog mode is still unknown. Returns
     * an actual changelog mode, when the PTF implements the {@link ChangelogFunction} interface.
     */
    default Optional<ChangelogMode> getOutputChangelogMode() {
        return Optional.empty();
    }

    /**
     * Returns the function's name usually referencing the function in a catalog.
     *
     * <p>Note: The name is meant for debugging purposes only.
     */
    String getName();

    /**
     * Returns a resolved list of the call's argument types. It also includes a type for every
     * argument in a vararg function call.
     */
    List<DataType> getArgumentDataTypes();

    /**
     * Returns the inferred output data type of the function call.
     *
     * <p>It does this by inferring the input argument data type using {@link
     * ArgumentTypeStrategy#inferArgumentType(CallContext, int, boolean)} of a wrapping call (if
     * available) where this function call is an argument. For example, {@code
     * takes_string(this_function(NULL))} would lead to a {@link DataTypes#STRING()} because the
     * wrapping call expects a string argument.
     */
    Optional<DataType> getOutputDataType();

    /**
     * Creates a validation exception for exiting the type inference process with a meaningful
     * exception.
     */
    default ValidationException newValidationError(String message, Object... args) {
        final String formatted;
        if (args.length > 0) {
            formatted = String.format(message, args);
        } else {
            formatted = message;
        }
        return new ValidationException(formatted);
    }

    /**
     * Helper method for handling failures during the type inference process while considering the
     * {@code throwOnFailure} flag.
     *
     * <p>Shorthand for {@code if (throwOnFailure) throw ValidationException(...) else return
     * Optional.empty()}.
     */
    default <T> Optional<T> fail(boolean throwOnFailure, String message, Object... args) {
        if (throwOnFailure) {
            throw newValidationError(message, args);
        }
        return Optional.empty();
    }

    /**
     * Returns whether the function call happens as part of an aggregation that defines grouping
     * columns.
     *
     * <p>E.g. {@code SELECT COUNT(*) FROM t} is not a grouped aggregation but {@code SELECT
     * COUNT(*) FROM t GROUP BY k} is.
     */
    boolean isGroupedAggregation();
}
