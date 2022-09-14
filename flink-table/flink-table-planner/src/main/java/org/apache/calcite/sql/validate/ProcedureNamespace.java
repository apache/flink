/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.validate;

import org.apache.flink.annotation.Internal;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static java.util.Objects.requireNonNull;

/**
 * Namespace whose contents are defined by the result of a call to a user-defined procedure.
 *
 * <p>Note: Compared to Calcite, this class implements custom logic for dealing with collection
 * tables like {@code TABLE(function(...))} procedures. Compared to the SQL standard, Flink's table
 * functions can return arbitrary types that are wrapped into a ROW type if necessary. We don't
 * interpret ARRAY or MULTISET types as it would be standard.
 */
@Internal
public final class ProcedureNamespace extends AbstractNamespace {

    private final SqlValidatorScope scope;

    private final SqlCall call;

    ProcedureNamespace(
            SqlValidatorImpl validator,
            SqlValidatorScope scope,
            SqlCall call,
            SqlNode enclosingNode) {
        super(validator, enclosingNode);
        this.scope = scope;
        this.call = call;
    }

    public RelDataType validateImpl(RelDataType targetRowType) {
        validator.inferUnknownTypes(validator.unknownType, scope, call);
        // The result is ignored but the type is derived to trigger the validation
        validator.deriveTypeImpl(scope, call);
        final SqlOperator operator = call.getOperator();
        final SqlCallBinding callBinding = new SqlCallBinding(validator, scope, call);
        if (!(operator instanceof SqlTableFunction)) {
            throw new IllegalArgumentException(
                    "Argument must be a table function: " + operator.getNameAsId());
        }
        final SqlTableFunction tableFunction = (SqlTableFunction) operator;
        final SqlReturnTypeInference rowTypeInference = tableFunction.getRowTypeInference();
        final RelDataType rowRelDataType =
                requireNonNull(
                        rowTypeInference.inferReturnType(callBinding),
                        () -> "got null from inferReturnType for call " + callBinding.getCall());
        // For BridgingSqlFunction the type can still be atomic
        // and will be wrapped with a proper field alias
        return toStruct(rowRelDataType, getNode());
    }

    /** Converts a type to a struct if it is not already. */
    protected RelDataType toStruct(RelDataType type, SqlNode unnest) {
        if (type.isStruct()) {
            return validator.getTypeFactory().createTypeWithNullability(type, false);
        }
        return validator
                .getTypeFactory()
                .builder()
                .kind(type.getStructKind())
                .add(validator.deriveAlias(unnest, 0), type)
                .build();
    }

    public SqlNode getNode() {
        return call;
    }
}
