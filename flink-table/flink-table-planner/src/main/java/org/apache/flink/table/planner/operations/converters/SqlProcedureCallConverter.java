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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkSqlCallBinding;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlProcedure;
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext;
import org.apache.flink.table.planner.operations.PlannerCallProcedureOperation;
import org.apache.flink.table.planner.plan.utils.RexLiteralUtil;
import org.apache.flink.table.procedures.ProcedureDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInferenceUtil;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.ExplicitOperatorBinding;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.typeutils.LogicalRelDataTypeConverter.toRelDataType;

/**
 * A converter for call procedure node. The call procedure statement will be parsed to a SqlCall
 * wrapping SqlProcedureCallOperator as the operator by calcite. So, this converter will try to
 * recognize it's call procedure or not. If it's call procedure, convert it the corresponding
 * operation. Otherwise, return null directly.
 */
public class SqlProcedureCallConverter implements SqlNodeConverter<SqlNode> {

    @Override
    public Optional<EnumSet<SqlKind>> supportedSqlKinds() {
        return Optional.of(EnumSet.of(SqlKind.PROCEDURE_CALL));
    }

    @Override
    public Operation convertSqlNode(SqlNode sqlNode, ConvertContext context) {
        SqlCall callProcedure = (SqlCall) ((SqlCall) sqlNode).getOperandList().get(0);
        BridgingSqlProcedure sqlProcedure = (BridgingSqlProcedure) callProcedure.getOperator();
        SqlValidator sqlValidator = context.getSqlValidator();
        ProcedureDefinition procedureDefinition =
                new ProcedureDefinition(sqlProcedure.getContextResolveProcedure().getProcedure());

        FlinkSqlCallBinding sqlCallBinding =
                new FlinkSqlCallBinding(
                        context.getSqlValidator(),
                        ((SqlValidatorImpl) context.getSqlValidator()).getEmptyScope(),
                        callProcedure);

        List<RexNode> reducedOperands = reduceOperands(sqlCallBinding, context);
        SqlOperatorBinding sqlOperatorBinding =
                new ExplicitOperatorBinding(
                        context.getSqlValidator().getTypeFactory(),
                        sqlProcedure,
                        reducedOperands.stream()
                                .map(RexNode::getType)
                                .collect(Collectors.toList()));

        OperatorBindingCallContext bindingCallContext =
                new OperatorBindingCallContext(
                        context.getCatalogManager().getDataTypeFactory(),
                        procedureDefinition,
                        sqlOperatorBinding,
                        sqlValidator.getValidatedNodeType(callProcedure));

        // run type inference to infer the type including types of input args
        // and output
        TypeInferenceUtil.Result typeInferResult =
                TypeInferenceUtil.runTypeInference(
                        procedureDefinition.getTypeInference(
                                context.getCatalogManager().getDataTypeFactory()),
                        bindingCallContext,
                        null);
        List<DataType> argumentTypes = typeInferResult.getExpectedArgumentTypes();
        int argumentCount = argumentTypes.size();
        DataType[] inputTypes = new DataType[argumentCount];
        Object[] params = new Object[argumentCount];
        for (int i = 0; i < argumentCount; i++) {
            inputTypes[i] = argumentTypes.get(i);
            RexNode reducedOperand = reducedOperands.get(i);
            if (!(reducedOperand instanceof RexLiteral)) {
                throw new ValidationException(
                        String.format(
                                "The argument at position %s %s for calling procedure can't be converted to "
                                        + "literal.",
                                i, context.toQuotedSqlString(callProcedure.operand(i))));
            }

            // convert the literal to Flink internal representation
            RexLiteral literalOperand = (RexLiteral) reducedOperand;
            Object internalValue =
                    RexLiteralUtil.toFlinkInternalValue(
                            literalOperand.getValueAs(Comparable.class),
                            inputTypes[i].getLogicalType());
            params[i] = internalValue;
        }
        return new PlannerCallProcedureOperation(
                sqlProcedure.getContextResolveProcedure().getIdentifier().getIdentifier().get(),
                sqlProcedure.getContextResolveProcedure().getProcedure(),
                params,
                inputTypes,
                typeInferResult.getOutputDataType());
    }

    private List<RexNode> reduceOperands(
            FlinkSqlCallBinding sqlCallBinding, ConvertContext context) {
        // we don't really care about the input row type while converting to RexNode
        // since call procedure shouldn't refer any inputs.
        // so, construct an empty row for it.
        RelDataType inputRowType =
                toRelDataType(
                        DataTypes.ROW().getLogicalType(),
                        context.getSqlValidator().getTypeFactory());
        return context.reduceRexNodes(
                sqlCallBinding.operands().stream()
                        .map(node -> context.toRexNode(node, inputRowType, null))
                        .collect(Collectors.toList()));
    }
}
