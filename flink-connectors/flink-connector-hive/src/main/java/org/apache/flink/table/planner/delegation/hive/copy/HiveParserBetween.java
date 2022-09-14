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

package org.apache.flink.table.planner.delegation.hive.copy;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

/** Counterpart of hive's org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveBetween. */
public class HiveParserBetween extends SqlSpecialOperator {

    public static final SqlSpecialOperator INSTANCE = new HiveParserBetween();

    private HiveParserBetween() {
        super(
                "BETWEEN",
                SqlKind.BETWEEN,
                30,
                true,
                ReturnTypes.BOOLEAN_NULLABLE,
                FIRST_BOOLEAN_THEN_FIRST_KNOWN,
                null);
    }

    /**
     * Operand type-inference strategy where an unknown operand type is derived from the first
     * operand with a known type, but the first operand is a boolean.
     */
    public static final SqlOperandTypeInference FIRST_BOOLEAN_THEN_FIRST_KNOWN =
            (callBinding, returnType, operandTypes) -> {
                final RelDataType unknownType = callBinding.getValidator().getUnknownType();
                RelDataType knownType = unknownType;
                for (int i = 1; i < callBinding.getCall().getOperandList().size(); i++) {
                    SqlNode operand = callBinding.getCall().getOperandList().get(i);
                    knownType =
                            callBinding.getValidator().deriveType(callBinding.getScope(), operand);
                    if (!knownType.equals(unknownType)) {
                        break;
                    }
                }

                RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
                operandTypes[0] = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                for (int i = 1; i < operandTypes.length; ++i) {
                    operandTypes[i] = knownType;
                }
            };
}
