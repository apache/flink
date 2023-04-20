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

package org.apache.flink.table.planner.functions.sql;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.functions.BuiltInFunctionDefinition.DEFAULT_VERSION;
import static org.apache.flink.table.functions.BuiltInFunctionDefinition.qualifyFunctionName;
import static org.apache.flink.table.functions.BuiltInFunctionDefinition.validateFunction;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * SQL version of {@link BuiltInFunctionDefinition} in cases where {@link BridgingSqlFunction} does
 * not apply. This is the case when the operator has a special parsing syntax or uses other
 * Calcite-specific features that are not exposed via {@link BuiltInFunctionDefinition} yet.
 *
 * <p>Note: Try to keep usages of this class to a minimum and use Flink's {@link
 * BuiltInFunctionDefinition} stack instead.
 *
 * <p>For simple functions, use the provided builder. Otherwise, this class can also be extended.
 */
@Internal
public class BuiltInSqlFunction extends SqlFunction implements BuiltInSqlOperator {

    private final @Nullable Integer version;

    private final boolean isDeterministic;

    private final boolean isInternal;

    private final Function<SqlOperatorBinding, SqlMonotonicity> monotonicity;

    protected BuiltInSqlFunction(
            String name,
            int version,
            SqlKind kind,
            @Nullable SqlReturnTypeInference returnTypeInference,
            @Nullable SqlOperandTypeInference operandTypeInference,
            @Nullable SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory category,
            boolean isDeterministic,
            boolean isInternal,
            Function<SqlOperatorBinding, SqlMonotonicity> monotonicity) {
        super(
                checkNotNull(name),
                checkNotNull(kind),
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                checkNotNull(category));
        this.version = isInternal ? null : version;
        this.isDeterministic = isDeterministic;
        this.isInternal = isInternal;
        this.monotonicity = monotonicity;
        validateFunction(name, version, isInternal);
    }

    protected BuiltInSqlFunction(
            String name,
            SqlKind kind,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            @Nullable SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory category) {
        this(
                name,
                DEFAULT_VERSION,
                kind,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                category,
                true,
                false,
                call -> SqlMonotonicity.NOT_MONOTONIC);
    }

    /** Builder for configuring and creating instances of {@link BuiltInSqlFunction}. */
    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public final Optional<Integer> getVersion() {
        return Optional.ofNullable(version);
    }

    @Override
    public String getQualifiedName() {
        if (isInternal) {
            return getName();
        }
        assert version != null;
        return qualifyFunctionName(getName(), version);
    }

    @Override
    public boolean isDeterministic() {
        return isDeterministic;
    }

    @Override
    public final boolean isInternal() {
        return isInternal;
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return monotonicity.apply(call);
    }

    // --------------------------------------------------------------------------------------------
    // Builder
    // --------------------------------------------------------------------------------------------

    /** Builder for fluent definition of built-in functions. */
    public static class Builder {

        private String name;

        private int version = DEFAULT_VERSION;

        private SqlKind kind = SqlKind.OTHER_FUNCTION;

        private SqlReturnTypeInference returnTypeInference;

        private SqlOperandTypeInference operandTypeInference;

        private SqlOperandTypeChecker operandTypeChecker;

        private SqlFunctionCategory category = SqlFunctionCategory.SYSTEM;

        private boolean isInternal = false;

        private boolean isDeterministic = true;

        private Function<SqlOperatorBinding, SqlMonotonicity> monotonicity =
                call -> SqlMonotonicity.NOT_MONOTONIC;

        /** @see BuiltInFunctionDefinition.Builder#name(String) */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /** @see BuiltInFunctionDefinition.Builder#version(int) */
        public Builder version(int version) {
            this.version = version;
            return this;
        }

        public Builder kind(SqlKind kind) {
            this.kind = kind;
            return this;
        }

        public Builder returnType(SqlReturnTypeInference returnTypeInference) {
            this.returnTypeInference = returnTypeInference;
            return this;
        }

        public Builder operandTypeInference(SqlOperandTypeInference operandTypeInference) {
            this.operandTypeInference = operandTypeInference;
            return this;
        }

        public Builder operandTypeChecker(SqlOperandTypeChecker operandTypeChecker) {
            this.operandTypeChecker = operandTypeChecker;
            return this;
        }

        public Builder category(SqlFunctionCategory category) {
            this.category = category;
            return this;
        }

        public Builder notDeterministic() {
            this.isDeterministic = false;
            return this;
        }

        /** @see BuiltInFunctionDefinition.Builder#internal() */
        public Builder internal() {
            this.isInternal = true;
            return this;
        }

        public Builder monotonicity(SqlMonotonicity staticMonotonicity) {
            this.monotonicity = call -> staticMonotonicity;
            return this;
        }

        public Builder monotonicity(Function<SqlOperatorBinding, SqlMonotonicity> monotonicity) {
            this.monotonicity = monotonicity;
            return this;
        }

        public BuiltInSqlFunction build() {
            return new BuiltInSqlFunction(
                    name,
                    version,
                    kind,
                    returnTypeInference,
                    operandTypeInference,
                    operandTypeChecker,
                    category,
                    isDeterministic,
                    isInternal,
                    monotonicity);
        }
    }
}
