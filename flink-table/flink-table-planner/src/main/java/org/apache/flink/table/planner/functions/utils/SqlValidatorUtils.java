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

package org.apache.flink.table.planner.functions.utils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.calcite.util.Static.RESOURCE;

/** Utility methods related to SQL validation. */
public class SqlValidatorUtils {

    public static void adjustTypeForArrayConstructor(
            RelDataType componentType, SqlOperatorBinding opBinding) {
        if (opBinding instanceof SqlCallBinding) {
            adjustTypeForMultisetConstructor(
                    componentType, componentType, (SqlCallBinding) opBinding);
        }
    }

    public static void adjustTypeForMapConstructor(
            Pair<RelDataType, RelDataType> componentType, SqlOperatorBinding opBinding) {
        if (opBinding instanceof SqlCallBinding) {
            adjustTypeForMultisetConstructor(
                    componentType.getKey(), componentType.getValue(), (SqlCallBinding) opBinding);
        }
    }

    public static boolean throwValidationSignatureErrorOrReturnFalse(
            SqlCallBinding callBinding, boolean throwOnFailure) {
        if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        } else {
            return false;
        }
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static boolean throwExceptionOrReturnFalse(
            Optional<RuntimeException> e, boolean throwOnFailure) {
        if (e.isPresent()) {
            if (throwOnFailure) {
                throw e.get();
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    /**
     * Checks whether the heading operands are in the form {@code (ROW, DESCRIPTOR, DESCRIPTOR ...,
     * other params)}, returning whether successful, and throwing if any columns are not found.
     *
     * @param callBinding The call binding
     * @param descriptorLocations position of the descriptor operands
     * @return true if validation passes; throws if any columns are not found
     */
    public static boolean checkTableAndDescriptorOperands(
            SqlCallBinding callBinding, Integer... descriptorLocations) {
        final SqlNode operand0 = callBinding.operand(0);
        final SqlValidator validator = callBinding.getValidator();
        final RelDataType type = validator.getValidatedNodeType(operand0);
        if (type.getSqlTypeName() != SqlTypeName.ROW) {
            return false;
        }
        for (Integer location : descriptorLocations) {
            final SqlNode operand = callBinding.operand(location);
            if (operand.getKind() != SqlKind.DESCRIPTOR) {
                return false;
            }
            validateColumnNames(
                    validator, type.getFieldNames(), ((SqlCall) operand).getOperandList());
        }
        return true;
    }

    private static void validateColumnNames(
            SqlValidator validator, List<String> fieldNames, List<SqlNode> columnNames) {
        final SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();
        for (SqlNode columnName : columnNames) {
            SqlIdentifier columnIdentifier = (SqlIdentifier) columnName;
            if (!columnIdentifier.isSimple()) {
                throw SqlUtil.newContextException(
                        columnName.getParserPosition(), RESOURCE.aliasMustBeSimpleIdentifier());
            }

            final String name = columnIdentifier.getSimple();
            if (matcher.indexOf(fieldNames, name) < 0) {
                throw SqlUtil.newContextException(
                        columnName.getParserPosition(), RESOURCE.unknownIdentifier(name));
            }
        }
    }

    /**
     * When the element element does not equal with the component type, making explicit casting.
     *
     * @param evenType derived type for element with even index
     * @param oddType derived type for element with odd index
     * @param sqlCallBinding description of call
     */
    private static void adjustTypeForMultisetConstructor(
            RelDataType evenType, RelDataType oddType, SqlCallBinding sqlCallBinding) {
        SqlCall call = sqlCallBinding.getCall();
        List<RelDataType> operandTypes = sqlCallBinding.collectOperandTypes();
        List<SqlNode> operands = call.getOperandList();
        RelDataType elementType;
        for (int i = 0; i < operands.size(); i++) {
            if (i % 2 == 0) {
                elementType = evenType;
            } else {
                elementType = oddType;
            }
            if (operandTypes.get(i).equalsSansFieldNames(elementType)) {
                continue;
            }
            call.setOperand(i, castTo(operands.get(i), elementType));
        }
    }

    /**
     * Make output field names unique from input field names by appending index. For example, Input
     * has field names {@code a, b, c} and output has field names {@code b, c, d}. After calling
     * this function, new output field names will be {@code b0, c0, d}. Duplicate names are not
     * checked inside input and output itself.
     *
     * @param input Input fields
     * @param output Output fields
     * @return
     */
    public static List<RelDataTypeField> makeOutputUnique(
            List<RelDataTypeField> input, List<RelDataTypeField> output) {
        final Set<String> inputFieldNames = new HashSet<>();
        for (RelDataTypeField field : input) {
            inputFieldNames.add(field.getName());
        }

        List<RelDataTypeField> result = new ArrayList<>();
        for (RelDataTypeField field : output) {
            String fieldName = field.getName();
            if (inputFieldNames.contains(fieldName)) {
                fieldName += "0"; // Append index to make it unique
            }
            result.add(new RelDataTypeFieldImpl(fieldName, field.getIndex(), field.getType()));
        }
        return result;
    }

    private static SqlNode castTo(SqlNode node, RelDataType type) {
        return SqlStdOperatorTable.CAST.createCall(
                SqlParserPos.ZERO,
                node,
                SqlTypeUtil.convertTypeToSpec(type).withNullable(type.isNullable()));
    }
}
