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

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.TableCharacteristic;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

/**
 * SqlSessionTableFunction implements an operator for per-key sessionization. It allows four
 * parameters:
 *
 * <ol>
 *   <li>table as data source
 *   <li>a descriptor to provide a watermarked column name from the input table
 *   <li>a descriptor to provide a column as key, on which sessionization will be applied, optional
 *   <li>an interval parameter to specify a inactive activity gap to break sessions
 * </ol>
 *
 * <p>Mainly copy from {@link org.apache.calcite.sql.SqlSessionTableFunction}. FLINK modifications
 * are the following:
 *
 * <ol>
 *   <li>Add some attributes required for PTF.
 *   <li>Invalid the deprecated syntax in calcite about 'partition by' in window tvf like: (TABLE
 *       table_name, DESCRIPTOR(timecol), DESCRIPTOR(key) optional, datetime interval)
 * </ol>
 */
public class SqlSessionTableFunction extends SqlWindowTableFunction {
    public SqlSessionTableFunction() {
        super(SqlKind.SESSION.name(), new OperandMetadataImpl());
    }

    private final Map<Integer, TableCharacteristic> tableParams =
            ImmutableMap.of(
                    0,
                    TableCharacteristic.builder(TableCharacteristic.Semantics.SET)
                            .pruneIfEmpty()
                            .passColumnsThrough()
                            .build());

    /** Operand type checker for SESSION. */
    private static class OperandMetadataImpl extends AbstractOperandMetadata {
        OperandMetadataImpl() {
            super(ImmutableList.of(PARAM_DATA, PARAM_TIMECOL, GAP), 3);
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            if (!checkTableAndDescriptorOperands(callBinding, 1)) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
            }

            final SqlValidator validator = callBinding.getValidator();
            final SqlNode operand2 = callBinding.operand(2);
            final RelDataType type2 = validator.getValidatedNodeType(operand2);
            if (!SqlTypeUtil.isInterval(type2)) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
            }

            return throwExceptionOrReturnFalse(
                    checkTimeColumnDescriptorOperand(callBinding, 1), throwOnFailure);
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
            return opName
                    + "(TABLE table_name [PARTITION BY (keycols, ...)], "
                    + "DESCRIPTOR(timecol), datetime interval)";
        }
    }

    @Override
    public @Nullable TableCharacteristic tableCharacteristic(int ordinal) {
        return tableParams.get(ordinal);
    }
}
