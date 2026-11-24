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

import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

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
 *
 * <p>The builder either defines a self-contained function ({@link #newBuilder()}) or a versioned
 * copy of an existing {@link SqlFunction} ({@link #newBuilder(SqlFunction)}). A copy behaves like
 * the wrapped function during planning and follows the same rules as {@link BuiltInSqlOperator}:
 * its name includes the version (e.g. {@code $CURRENT_DATE$1}) and older versions should be
 * internal. See {@link FlinkSqlOperatorTable#VERSIONED_OPERATORS} for how to version a function
 * that cannot be replaced by a self-contained one.
 */
@Internal
public class BuiltInSqlFunction extends SqlFunction implements VersionedSqlOperator {

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
        validateFunction(name, this.version, isInternal);
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

    /** Builder for a versioned copy of the given function. */
    public static WrappingBuilder newBuilder(SqlFunction function) {
        return new WrappingBuilder(function);
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
    // Versioned copy
    // --------------------------------------------------------------------------------------------

    /** A versioned copy of an existing {@link SqlFunction} that delegates to it. */
    private static final class WrappingSqlFunction extends BuiltInSqlFunction {

        private final SqlFunction function;

        private WrappingSqlFunction(SqlFunction function, int version, boolean isInternal) {
            super(
                    qualifyFunctionName(function.getName(), version),
                    version,
                    function.getKind(),
                    function.getReturnTypeInference(),
                    function.getOperandTypeInference(),
                    function.getOperandTypeChecker(),
                    function.getFunctionType(),
                    function.isDeterministic(),
                    isInternal,
                    function::getMonotonicity);
            this.function = function;
        }

        @Override
        public String getQualifiedName() {
            return getName();
        }

        @Override
        public SqlSyntax getSyntax() {
            return function.getSyntax();
        }

        @Override
        public @Nullable SqlIdentifier getSqlIdentifier() {
            return function.getSqlIdentifier();
        }

        @Override
        public SqlFunctionCategory getFunctionType() {
            return function.getFunctionType();
        }

        @Override
        public boolean isQuantifierAllowed() {
            return function.isQuantifierAllowed();
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return function.getOperandCountRange();
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            return function.checkOperandTypes(callBinding, throwOnFailure);
        }

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            return function.inferReturnType(opBinding);
        }

        @Override
        public RelDataType deriveType(
                SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            return function.deriveType(validator, scope, call);
        }

        @Override
        public void validateCall(
                SqlCall call,
                SqlValidator validator,
                SqlValidatorScope scope,
                SqlValidatorScope operandScope) {
            function.validateCall(call, validator, scope, operandScope);
        }

        @Override
        public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
            return function.rewriteCall(validator, call);
        }

        @Override
        public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
            function.unparse(writer, call, leftPrec, rightPrec);
        }

        @Override
        public String getAllowedSignatures(String opNameToUse) {
            return function.getAllowedSignatures(opNameToUse);
        }

        @Override
        public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
            return function.getMonotonicity(call);
        }

        @Override
        public boolean isDeterministic() {
            return function.isDeterministic();
        }

        @Override
        public boolean isDynamicFunction() {
            return function.isDynamicFunction();
        }

        @Override
        public Supplier<Strong.Policy> getStrongPolicyInference() {
            return function.getStrongPolicyInference();
        }

        @Override
        public Boolean isSafeOperator() {
            return function.isSafeOperator();
        }
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

        /**
         * @see BuiltInFunctionDefinition.Builder#name(String)
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * @see BuiltInFunctionDefinition.Builder#version(int)
         */
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

        /**
         * @see BuiltInFunctionDefinition.Builder#internal()
         */
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

    /** Builder for fluent definition of a versioned copy of an existing function. */
    public static final class WrappingBuilder {

        private final SqlFunction function;

        private int version = DEFAULT_VERSION;

        private boolean isInternal = false;

        private WrappingBuilder(SqlFunction function) {
            this.function = checkNotNull(function);
        }

        /**
         * @see BuiltInFunctionDefinition.Builder#version(int)
         */
        public WrappingBuilder version(int version) {
            this.version = version;
            return this;
        }

        /**
         * @see BuiltInFunctionDefinition.Builder#internal()
         */
        public WrappingBuilder internal() {
            this.isInternal = true;
            return this;
        }

        public BuiltInSqlFunction build() {
            return new WrappingSqlFunction(function, version, isInternal);
        }
    }
}
