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

package org.apache.calcite.sql;

import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.List;
import java.util.Objects;

import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A <code>SqlFunction</code> is a type of operator which has conventional function-call syntax.
 *
 * <p>This class has been copied to change the default for convertRowArgToColumnList to 'false' in
 * {@link SqlFunction#deriveType}. The class can be dropped once we upgrade to Calcite 1.36 where
 * the affected line has been updated as part of CALCITE-5644.
 */
public class SqlFunction extends SqlOperator {

    // ~ Instance fields --------------------------------------------------------

    private final SqlFunctionCategory category;

    private final @Nullable SqlIdentifier sqlIdentifier;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a new SqlFunction for a call to a built-in function.
     *
     * @param name Name of built-in function
     * @param kind kind of operator implemented by function
     * @param returnTypeInference strategy to use for return type inference
     * @param operandTypeInference strategy to use for parameter type inference
     * @param operandTypeChecker strategy to use for parameter type checking
     * @param category categorization for function
     */
    public SqlFunction(
            String name,
            SqlKind kind,
            @Nullable SqlReturnTypeInference returnTypeInference,
            @Nullable SqlOperandTypeInference operandTypeInference,
            @Nullable SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory category) {
        // We leave sqlIdentifier as null to indicate
        // that this is a built-in.
        this(
                name,
                null,
                kind,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                category);

        assert !((category == SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR)
                && (returnTypeInference == null));
    }

    /**
     * Creates a placeholder SqlFunction for an invocation of a function with a possibly qualified
     * name. This name must be resolved into either a built-in function or a user-defined function.
     *
     * @param sqlIdentifier possibly qualified identifier for function
     * @param returnTypeInference strategy to use for return type inference
     * @param operandTypeInference strategy to use for parameter type inference
     * @param operandTypeChecker strategy to use for parameter type checking
     * @param paramTypes array of parameter types
     * @param funcType function category
     */
    public SqlFunction(
            SqlIdentifier sqlIdentifier,
            @Nullable SqlReturnTypeInference returnTypeInference,
            @Nullable SqlOperandTypeInference operandTypeInference,
            @Nullable SqlOperandTypeChecker operandTypeChecker,
            @Nullable List<RelDataType> paramTypes,
            SqlFunctionCategory funcType) {
        this(
                Util.last(sqlIdentifier.names),
                sqlIdentifier,
                SqlKind.OTHER_FUNCTION,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                paramTypes,
                funcType);
    }

    @Deprecated // to be removed before 2.0
    protected SqlFunction(
            String name,
            @Nullable SqlIdentifier sqlIdentifier,
            SqlKind kind,
            @Nullable SqlReturnTypeInference returnTypeInference,
            @Nullable SqlOperandTypeInference operandTypeInference,
            @Nullable SqlOperandTypeChecker operandTypeChecker,
            @Nullable List<RelDataType> paramTypes,
            SqlFunctionCategory category) {
        this(
                name,
                sqlIdentifier,
                kind,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                category);
    }

    /** Internal constructor. */
    protected SqlFunction(
            String name,
            @Nullable SqlIdentifier sqlIdentifier,
            SqlKind kind,
            @Nullable SqlReturnTypeInference returnTypeInference,
            @Nullable SqlOperandTypeInference operandTypeInference,
            @Nullable SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory category) {
        super(name, kind, 100, 100, returnTypeInference, operandTypeInference, operandTypeChecker);

        this.sqlIdentifier = sqlIdentifier;
        this.category = Objects.requireNonNull(category, "category");
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.FUNCTION;
    }

    /** Returns the fully-qualified name of function, or null for a built-in function. */
    public @Nullable SqlIdentifier getSqlIdentifier() {
        return sqlIdentifier;
    }

    @Override
    public SqlIdentifier getNameAsId() {
        if (sqlIdentifier != null) {
            return sqlIdentifier;
        }
        return super.getNameAsId();
    }

    /**
     * Use {@link SqlOperandMetadata#paramTypes(RelDataTypeFactory)} on the result of {@link
     * #getOperandTypeChecker()}.
     */
    @Deprecated // to be removed before 2.0
    public @Nullable List<RelDataType> getParamTypes() {
        return null;
    }

    /**
     * Use {@link SqlOperandMetadata#paramNames()} on the result of {@link
     * #getOperandTypeChecker()}.
     */
    @Deprecated // to be removed before 2.0
    public List<String> getParamNames() {
        return Functions.generate(castNonNull(getParamTypes()).size(), i -> "arg" + i);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        getSyntax().unparse(writer, this, call, leftPrec, rightPrec);
    }

    /** Return function category. */
    public SqlFunctionCategory getFunctionType() {
        return this.category;
    }

    /**
     * Returns whether this function allows a <code>DISTINCT</code> or <code>
     * ALL</code> quantifier. The default is <code>false</code>; some aggregate functions return
     * <code>true</code>.
     */
    @Pure
    public boolean isQuantifierAllowed() {
        return false;
    }

    @Override
    public void validateCall(
            SqlCall call,
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlValidatorScope operandScope) {
        // This implementation looks for the quantifier keywords DISTINCT or
        // ALL as the first operand in the list.  If found then the literal is
        // not called to validate itself.  Further the function is checked to
        // make sure that a quantifier is valid for that particular function.
        //
        // If the first operand does not appear to be a quantifier then the
        // parent ValidateCall is invoked to do normal function validation.

        super.validateCall(call, validator, scope, operandScope);
        validateQuantifier(validator, call);
    }

    /** Throws a validation error if a DISTINCT or ALL quantifier is present but not allowed. */
    protected void validateQuantifier(SqlValidator validator, SqlCall call) {
        SqlLiteral functionQuantifier = call.getFunctionQuantifier();
        if ((null != functionQuantifier) && !isQuantifierAllowed()) {
            throw validator.newValidationError(
                    functionQuantifier,
                    RESOURCE.functionQuantifierNotAllowed(call.getOperator().getName()));
        }
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        // ----- FLINK MODIFICATION BEGIN -----
        return deriveType(validator, scope, call, false);
        // ----- FLINK MODIFICATION END -----
    }

    private RelDataType deriveType(
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlCall call,
            boolean convertRowArgToColumnList) {
        // Scope for operands. Usually the same as 'scope'.
        final SqlValidatorScope operandScope = scope.getOperandScope(call);

        // Indicate to the validator that we're validating a new function call
        validator.pushFunctionCall();

        final List<String> argNames = constructArgNameList(call);

        final List<SqlNode> args = constructOperandList(validator, call, argNames);

        final List<RelDataType> argTypes =
                constructArgTypeList(validator, scope, call, args, convertRowArgToColumnList);

        SqlFunction function =
                (SqlFunction)
                        SqlUtil.lookupRoutine(
                                validator.getOperatorTable(),
                                validator.getTypeFactory(),
                                getNameAsId(),
                                argTypes,
                                argNames,
                                getFunctionType(),
                                SqlSyntax.FUNCTION,
                                getKind(),
                                validator.getCatalogReader().nameMatcher(),
                                false);
        try {
            // if we have a match on function name and parameter count, but
            // couldn't find a function with  a COLUMN_LIST type, retry, but
            // this time, don't convert the row argument to a COLUMN_LIST type;
            // if we did find a match, go back and re-validate the row operands
            // (corresponding to column references), now that we can set the
            // scope to that of the source cursor referenced by that ColumnList
            // type
            if (convertRowArgToColumnList && containsRowArg(args)) {
                if (function == null
                        && SqlUtil.matchRoutinesByParameterCount(
                                validator.getOperatorTable(),
                                getNameAsId(),
                                argTypes,
                                getFunctionType(),
                                validator.getCatalogReader().nameMatcher())) {
                    // remove the already validated node types corresponding to
                    // row arguments before re-validating
                    for (SqlNode operand : args) {
                        if (operand.getKind() == SqlKind.ROW) {
                            validator.removeValidatedNodeType(operand);
                        }
                    }
                    return deriveType(validator, scope, call, false);
                } else if (function != null) {
                    validator.validateColumnListParams(function, argTypes, args);
                }
            }

            if (getFunctionType() == SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR) {
                return validator.deriveConstructorType(scope, call, this, function, argTypes);
            }

            validCoercionType:
            if (function == null) {
                if (validator.config().typeCoercionEnabled()) {
                    // try again if implicit type coercion is allowed.
                    function =
                            (SqlFunction)
                                    SqlUtil.lookupRoutine(
                                            validator.getOperatorTable(),
                                            validator.getTypeFactory(),
                                            getNameAsId(),
                                            argTypes,
                                            argNames,
                                            getFunctionType(),
                                            SqlSyntax.FUNCTION,
                                            getKind(),
                                            validator.getCatalogReader().nameMatcher(),
                                            true);
                    // try to coerce the function arguments to the declared sql type name.
                    // if we succeed, the arguments would be wrapped with CAST operator.
                    if (function != null) {
                        TypeCoercion typeCoercion = validator.getTypeCoercion();
                        if (typeCoercion.userDefinedFunctionCoercion(scope, call, function)) {
                            break validCoercionType;
                        }
                    }
                }

                // check if the identifier represents type
                final SqlFunction x = (SqlFunction) call.getOperator();
                final SqlIdentifier identifier =
                        Util.first(
                                x.getSqlIdentifier(),
                                new SqlIdentifier(x.getName(), SqlParserPos.ZERO));
                RelDataType type = validator.getCatalogReader().getNamedType(identifier);
                if (type != null) {
                    function = new SqlTypeConstructorFunction(identifier, type);
                    break validCoercionType;
                }

                // if function doesn't exist within operator table and known function
                // handling is turned off then create a more permissive function
                if (function == null && validator.config().lenientOperatorLookup()) {
                    function =
                            new SqlUnresolvedFunction(
                                    identifier,
                                    null,
                                    null,
                                    OperandTypes.VARIADIC,
                                    null,
                                    x.getFunctionType());
                    break validCoercionType;
                }
                throw validator.handleUnresolvedFunction(call, this, argTypes, argNames);
            }

            // REVIEW jvs 25-Mar-2005:  This is, in a sense, expanding
            // identifiers, but we ignore shouldExpandIdentifiers()
            // because otherwise later validation code will
            // choke on the unresolved function.
            ((SqlBasicCall) call).setOperator(function);
            return function.validateOperands(validator, operandScope, call);
        } finally {
            validator.popFunctionCall();
        }
    }

    private static boolean containsRowArg(List<SqlNode> args) {
        for (SqlNode operand : args) {
            if (operand.getKind() == SqlKind.ROW) {
                return true;
            }
        }
        return false;
    }
}
