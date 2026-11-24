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

import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.flink.table.functions.BuiltInFunctionDefinition.DEFAULT_VERSION;
import static org.apache.flink.table.functions.BuiltInFunctionDefinition.qualifyFunctionName;
import static org.apache.flink.table.functions.BuiltInFunctionDefinition.validateFunction;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A versioned copy of an existing {@link SqlOperator} that cannot be replaced by a {@link
 * BuiltInSqlFunction}, for example because Calcite references it directly in the parser or {@code
 * RexBuilder}. Use {@link BuiltInSqlFunction#newBuilder(SqlFunction)} for a {@link SqlFunction}.
 *
 * <p>The operator behaves like the wrapped operator during planning. The latest version only
 * defines the version of the wrapped operator in {@link FlinkSqlOperatorTable#VERSIONED_OPERATORS}.
 * Older versions are registered in {@link FlinkSqlOperatorTable} for restoring compiled plans and
 * must be matched on during code generation, see {@link FlinkSqlOperatorTable#VERSIONED_OPERATORS}
 * for details.
 *
 * <p>The name includes the version (e.g. {@code $ITEM$1}), which makes older versions accessible
 * for restoring compiled plans without conflicting with the wrapped operator. Similar to {@link
 * BuiltInFunctionDefinition}, older versions should be internal to not expose them when listing
 * functions. For internal operators, the version is only part of the name.
 */
@Internal
public final class BuiltInSqlOperator extends SqlOperator implements VersionedSqlOperator {

    private final SqlOperator operator;

    private final @Nullable Integer version;

    private final boolean isInternal;

    private BuiltInSqlOperator(SqlOperator operator, int version, boolean isInternal) {
        super(
                qualifyFunctionName(operator.getName(), version),
                operator.getKind(),
                operator.getLeftPrec(),
                operator.getRightPrec(),
                operator.getReturnTypeInference(),
                operator.getOperandTypeInference(),
                operator.getOperandTypeChecker());
        this.operator = operator;
        this.version = isInternal ? null : version;
        this.isInternal = isInternal;
        validateFunction(isInternal ? getName() : operator.getName(), this.version, isInternal);
    }

    /** Builder for a versioned copy of the given operator. */
    public static Builder newBuilder(SqlOperator operator) {
        return new Builder(operator);
    }

    @Override
    public Optional<Integer> getVersion() {
        return Optional.ofNullable(version);
    }

    @Override
    public boolean isInternal() {
        return isInternal;
    }

    @Override
    public String getQualifiedName() {
        return getName();
    }

    // --------------------------------------------------------------------------------------------
    // Delegation to the wrapped operator
    // --------------------------------------------------------------------------------------------

    @Override
    public SqlSyntax getSyntax() {
        return operator.getSyntax();
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return operator.getOperandCountRange();
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        return operator.checkOperandTypes(callBinding, throwOnFailure);
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        return operator.inferReturnType(opBinding);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        return operator.deriveType(validator, scope, call);
    }

    @Override
    public void validateCall(
            SqlCall call,
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlValidatorScope operandScope) {
        operator.validateCall(call, validator, scope, operandScope);
    }

    @Override
    public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
        return operator.rewriteCall(validator, call);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        operator.unparse(writer, call, leftPrec, rightPrec);
    }

    @Override
    public String getAllowedSignatures(String opNameToUse) {
        return operator.getAllowedSignatures(opNameToUse);
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return operator.getMonotonicity(call);
    }

    @Override
    public boolean isDeterministic() {
        return operator.isDeterministic();
    }

    @Override
    public boolean isDynamicFunction() {
        return operator.isDynamicFunction();
    }

    @Override
    public Supplier<Strong.Policy> getStrongPolicyInference() {
        return operator.getStrongPolicyInference();
    }

    @Override
    public Boolean isSafeOperator() {
        return operator.isSafeOperator();
    }

    // --------------------------------------------------------------------------------------------
    // Builder
    // --------------------------------------------------------------------------------------------

    /** Builder for fluent definition of versioned built-in operators. */
    public static final class Builder {

        private final SqlOperator operator;

        private int version = DEFAULT_VERSION;

        private boolean isInternal = false;

        private Builder(SqlOperator operator) {
            this.operator = checkNotNull(operator);
            checkArgument(
                    !(operator instanceof SqlFunction),
                    "Use BuiltInSqlFunction.newBuilder(SqlFunction) for versioning function '%s'.",
                    operator.getName());
        }

        /**
         * @see BuiltInFunctionDefinition.Builder#version(int)
         */
        public Builder version(int version) {
            this.version = version;
            return this;
        }

        /**
         * @see BuiltInFunctionDefinition.Builder#internal()
         */
        public Builder internal() {
            this.isInternal = true;
            return this;
        }

        public BuiltInSqlOperator build() {
            return new BuiltInSqlOperator(operator, version, isInternal);
        }
    }
}
