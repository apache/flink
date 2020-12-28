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

package org.apache.flink.table.planner.functions.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;

import javax.annotation.Nullable;

import java.util.AbstractList;
import java.util.List;
import java.util.Optional;

/**
 * A {@link CallContext} backed by {@link SqlCallBinding}. Compared to {@link
 * OperatorBindingCallContext}, this class is able to reorder arguments.
 */
@Internal
public final class CallBindingCallContext extends AbstractSqlCallContext {

    private final List<SqlNode> adaptedArguments;

    private final List<DataType> argumentDataTypes;

    private final @Nullable DataType outputType;

    public CallBindingCallContext(
            DataTypeFactory dataTypeFactory,
            FunctionDefinition definition,
            SqlCallBinding binding,
            @Nullable RelDataType outputType) {
        super(dataTypeFactory, definition, binding.getOperator().getNameAsId().toString());

        this.adaptedArguments = binding.operands(); // reorders the operands
        this.argumentDataTypes =
                new AbstractList<DataType>() {
                    @Override
                    public DataType get(int pos) {
                        final RelDataType relDataType =
                                binding.getValidator()
                                        .deriveType(binding.getScope(), adaptedArguments.get(pos));
                        final LogicalType logicalType = FlinkTypeFactory.toLogicalType(relDataType);
                        return TypeConversions.fromLogicalToDataType(logicalType);
                    }

                    @Override
                    public int size() {
                        return binding.getOperandCount();
                    }
                };
        this.outputType = convertOutputType(binding, outputType);
    }

    @Override
    public boolean isArgumentLiteral(int pos) {
        return SqlUtil.isLiteral(adaptedArguments.get(pos), false);
    }

    @Override
    public boolean isArgumentNull(int pos) {
        return SqlUtil.isNullLiteral(adaptedArguments.get(pos), false);
    }

    @Override
    public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
        if (isArgumentNull(pos)) {
            return Optional.empty();
        }
        try {
            final SqlLiteral literal = SqlLiteral.unchain(adaptedArguments.get(pos));
            return Optional.ofNullable(getLiteralValueAs(literal::getValueAs, clazz));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<DataType> getArgumentDataTypes() {
        return argumentDataTypes;
    }

    @Override
    public Optional<DataType> getOutputDataType() {
        return Optional.ofNullable(outputType);
    }

    // --------------------------------------------------------------------------------------------

    private static @Nullable DataType convertOutputType(
            SqlCallBinding binding, @Nullable RelDataType returnType) {
        if (returnType == null || returnType.equals(binding.getValidator().getUnknownType())) {
            return null;
        } else {
            final LogicalType logicalType = FlinkTypeFactory.toLogicalType(returnType);
            return TypeConversions.fromLogicalToDataType(logicalType);
        }
    }
}
