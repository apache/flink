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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;

import java.io.Serializable;
import java.lang.invoke.MethodHandle;

/**
 * A {@link FunctionDefinition} that can provide a runtime implementation (i.e. the function's body)
 * that is specialized for the given call and session.
 *
 * <p>The planner tries to defer the specialization until shortly before code generation, where the
 * information given by a {@link FunctionDefinition} is not enough anymore and a subclass of {@link
 * UserDefinedFunction} is required for runtime.
 *
 * <p>This interface is useful when the runtime code should know about information that is only
 * available after planning (e.g. local session time zone or precision/scale of decimal return
 * type).
 *
 * <p>A {@link UserDefinedFunction} that is registered in the API is implicitly specialized but can
 * also implement this interface to reconfigure itself before runtime.
 *
 * <p>Note: This interface is a rather low level kind of function but useful for advanced users.
 */
@PublicEvolving
public interface SpecializedFunction extends FunctionDefinition {

    /**
     * Provides a runtime implementation that is specialized for the given call and session.
     *
     * <p>The method must return an instance of {@link UserDefinedFunction} or throw a {@link
     * TableException} if the given call is not supported. The returned instance must have the same
     * {@link FunctionDefinition} semantics but can have a different {@link
     * #getTypeInference(DataTypeFactory)} implementation.
     */
    UserDefinedFunction specialize(SpecializedContext context);

    /** Provides call and session information for the specialized function. */
    @PublicEvolving
    interface SpecializedContext extends ExpressionEvaluatorFactory {

        /** Returns the context of the current call. */
        CallContext getCallContext();

        /** Gives read-only access to the configuration of the current session. */
        ReadableConfig getConfiguration();

        /** Returns the classloader used to resolve built-in functions. */
        ClassLoader getBuiltInClassLoader();
    }

    /** Helper interface for creating {@link ExpressionEvaluator}s. */
    @PublicEvolving
    interface ExpressionEvaluatorFactory {

        /**
         * Creates a serializable factory that can be passed into a {@link UserDefinedFunction} for
         * evaluating an {@link Expression} during runtime.
         *
         * <p>Add a dependency to the {@code flink-table-api-java} module to access all available
         * expressions of Table API.
         *
         * <p>Initialize the evaluator in {@link UserDefinedFunction#open(FunctionContext)} by
         * calling {@link ExpressionEvaluator#open(FunctionContext)}. It will return an invokable
         * instance to be called during runtime.
         */
        ExpressionEvaluator createEvaluator(
                Expression expression, DataType outputDataType, DataTypes.Field... args);

        /**
         * Shorthand for {@code createEvaluator(callSql("..."), ...)}.
         *
         * @see #createEvaluator(String, DataType, DataTypes.Field...)
         */
        ExpressionEvaluator createEvaluator(
                String sqlExpression, DataType outputDataType, DataTypes.Field... args);

        /**
         * Creates a serializable factory that can be passed into a {@link UserDefinedFunction} for
         * evaluating a {@link BuiltInFunctionDefinition} during runtime.
         *
         * <p>This method enables to call basic functions without a dependency to the API modules.
         * See {@link BuiltInFunctionDefinitions} for a list available functions.
         *
         * <p>Initialize the evaluator in {@link UserDefinedFunction#open(FunctionContext)} by
         * calling {@link ExpressionEvaluator#open(FunctionContext)}. It will return an invokable
         * instance to be called during runtime.
         */
        ExpressionEvaluator createEvaluator(
                BuiltInFunctionDefinition function, DataType outputDataType, DataType... args);
    }

    /**
     * Serializable factory that can be passed into a {@link UserDefinedFunction} for evaluating
     * previously defined expression during runtime.
     *
     * @see SpecializedContext#createEvaluator(Expression, DataType, DataTypes.Field...)
     * @see SpecializedContext#createEvaluator(BuiltInFunctionDefinition, DataType, DataType...)
     */
    @PublicEvolving
    interface ExpressionEvaluator extends Serializable {

        /**
         * Creates and initializes runtime implementation for expression evaluation. The returned
         * {@link MethodHandle} should be stored in a transient variable and can be invoked via
         * {@link MethodHandle#invokeExact(Object...)} using the conversion classes previously
         * defined via the passed {@link DataType}s.
         *
         * <p>This method should be called in {@link UserDefinedFunction#open(FunctionContext)}.
         */
        MethodHandle open(FunctionContext context);

        /**
         * Closes the runtime implementation for expression evaluation. It performs clean up work.
         *
         * <p>This method should be called in {@link UserDefinedFunction#close()}.
         */
        default void close() {}
    }
}
