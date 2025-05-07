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

import org.apache.flink.table.planner.catalog.CatalogSchemaModel;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.util.Static.RESOURCE;

/** SqlModelCall to fetch and reference model based on identifier. */
public class SqlModelCall extends SqlBasicCall {

    private @Nullable CatalogSchemaModel model = null;

    public SqlModelCall(SqlExplicitModelCall modelCall) {
        super(
                SqlModelOperator.create(
                        modelCall.getOperator().getName(),
                        modelCall.getOperator().getKind(),
                        modelCall.getOperator().getLeftPrec(),
                        modelCall.getOperator().getRightPrec(),
                        (SqlIdentifier) modelCall.getOperandList().get(0)),
                modelCall.getOperandList(),
                modelCall.getParserPosition(),
                modelCall.getFunctionQuantifier());
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (model != null) {
            return;
        }

        SqlIdentifier modelIdentifier = (SqlIdentifier) getOperandList().get(0);
        SqlValidatorCatalogReader catalogReader = validator.getCatalogReader();
        assert catalogReader instanceof FlinkCalciteCatalogReader;

        model = ((FlinkCalciteCatalogReader) catalogReader).getModel(modelIdentifier.names);
        if (model == null) {
            throw SqlUtil.newContextException(
                    modelIdentifier.getParserPosition(),
                    RESOURCE.objectNotFound(modelIdentifier.toString()));
        }
    }

    public RelDataType getInputType(SqlValidator validator) {
        assert model != null;
        return model.getOutputRowType(validator.getTypeFactory());
    }

    public RelDataType getOutputType(SqlValidator validator) {
        assert model != null;
        return model.getOutputRowType(validator.getTypeFactory());
    }

    /**
     * A custom SqlOperator to handle model identifier.
     *
     * <p>It is used to derive the type of the model based on the identifier.
     */
    private static class SqlModelOperator extends SqlOperator {

        private final SqlIdentifier modelIdentifier;

        private static SqlModelOperator create(
                String name,
                SqlKind kind,
                int leftPrecedence,
                int rightPrecedence,
                SqlIdentifier identifier) {
            return new SqlModelOperator(name, kind, leftPrecedence, rightPrecedence, identifier);
        }

        private SqlModelOperator(
                String name,
                SqlKind kind,
                int leftPrecedence,
                int rightPrecedence,
                SqlIdentifier identifier) {
            super(name, kind, leftPrecedence, rightPrecedence, null, null, null);
            this.modelIdentifier = identifier;
        }

        @Override
        public SqlSyntax getSyntax() {
            return SqlSyntax.PREFIX;
        }

        @Override
        public RelDataType deriveType(
                SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            SqlValidatorCatalogReader catalogReader = validator.getCatalogReader();
            assert catalogReader instanceof FlinkCalciteCatalogReader;

            CatalogSchemaModel model =
                    ((FlinkCalciteCatalogReader) catalogReader).getModel(modelIdentifier.names);
            if (model == null) {
                throw SqlUtil.newContextException(
                        modelIdentifier.getParserPosition(),
                        RESOURCE.objectNotFound(modelIdentifier.toString()));
            }
            return model.getOutputRowType(validator.getTypeFactory());
        }
    }
}
