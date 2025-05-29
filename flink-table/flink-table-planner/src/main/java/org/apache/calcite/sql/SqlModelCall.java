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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import static java.util.Objects.requireNonNull;

/** SqlModelCall to fetch and reference model based on identifier. */
public class SqlModelCall extends SqlBasicCall {

    private final CatalogSchemaModel model;

    public SqlModelCall(SqlExplicitModelCall modelCall, CatalogSchemaModel model) {
        super(
                new SqlModelOperator(model),
                modelCall.getOperandList(),
                modelCall.getParserPosition(),
                modelCall.getFunctionQuantifier());
        this.model = requireNonNull(model);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        // Do nothing here, override to avoid identifier validation which will be treated as column
    }

    public RelDataType getInputType(SqlValidator validator) {
        return model.getInputRowType(validator.getTypeFactory());
    }

    public RelDataType getOutputType(SqlValidator validator) {
        return model.getOutputRowType(validator.getTypeFactory());
    }

    public CatalogSchemaModel getModel() {
        return model;
    }

    /**
     * A custom SqlOperator to handle model identifier.
     *
     * <p>It is used to derive the type of the model based on the identifier.
     */
    private static class SqlModelOperator extends SqlPrefixOperator {

        CatalogSchemaModel model;

        private SqlModelOperator(CatalogSchemaModel model) {
            super("MODEL", SqlKind.OTHER_FUNCTION, 2, null, null, null);
            this.model = model;
        }

        @Override
        public RelDataType deriveType(
                SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            return model.getOutputRowType(validator.getTypeFactory());
        }
    }
}
