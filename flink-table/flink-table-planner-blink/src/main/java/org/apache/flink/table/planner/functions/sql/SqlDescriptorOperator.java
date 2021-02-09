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

package org.apache.flink.table.planner.functions.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * DESCRIPTOR appears as an argument in a function. DESCRIPTOR accepts a list of identifiers that
 * represent a list of names. The interpretation of names is left to the function.
 *
 * <p>A typical syntax is DESCRIPTOR(col_name, ...).
 *
 * <p>An example is a table-valued function that takes names of columns to filter on.
 *
 * <p>Note: we copied the implementation from Calcite's {@link
 * org.apache.calcite.sql.SqlDescriptorOperator}, but support forwarding column name information and
 * data types if available. This is important because we will generate an additional {@code
 * window_time} time attribute column which should keep the same type with original time attribute.
 */
public class SqlDescriptorOperator extends SqlOperator {

    public SqlDescriptorOperator() {
        super(
                "DESCRIPTOR",
                SqlKind.DESCRIPTOR,
                100,
                100,
                SqlDescriptorOperator::inferRowType,
                null,
                null);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        final RelDataTypeFactory typeFactory = validator.getTypeFactory();
        RelDataTypeFactory.Builder builder =
                typeFactory.builder().kind(StructKind.PEEK_FIELDS_NO_EXPAND);
        for (SqlNode node : call.getOperandList()) {
            if (node instanceof SqlIdentifier) {
                // we can't infer the column type, just forward the column name for now.
                builder.add(((SqlIdentifier) node).getSimple(), typeFactory.createUnknownType());
            } else {
                throw new IllegalArgumentException(
                        "DESCRIPTOR operator only supports accepting a list of "
                                + "identifiers that represent a list of names.");
            }
        }
        return builder.build();
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        for (SqlNode operand : callBinding.getCall().getOperandList()) {
            if (!(operand instanceof SqlIdentifier) || ((SqlIdentifier) operand).isSimple()) {
                if (throwOnFailure) {
                    throw SqlUtil.newContextException(
                            operand.getParserPosition(), RESOURCE.aliasMustBeSimpleIdentifier());
                }
            }
        }
        return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(1);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.FUNCTION;
    }

    private static RelDataType inferRowType(SqlOperatorBinding opBinding) {
        RelDataTypeFactory.Builder builder =
                opBinding.getTypeFactory().builder().kind(StructKind.PEEK_FIELDS_NO_EXPAND);
        for (int i = 0; i < opBinding.getOperandCount(); i++) {
            builder.add("$" + i, opBinding.getOperandType(i));
        }
        return builder.build();
    }
}
