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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;

import javax.annotation.Nullable;

import java.util.AbstractList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** A {@link CallContext} backed by {@link SqlOperatorBinding}. */
@Internal
public final class OperatorBindingCallContext extends AbstractSqlCallContext {

    private final SqlOperatorBinding binding;
    private final List<DataType> argumentDataTypes;
    private final @Nullable DataType outputDataType;
    private final @Nullable List<ChangelogMode> inputChangelogModes;

    public OperatorBindingCallContext(
            DataTypeFactory dataTypeFactory,
            FunctionDefinition definition,
            SqlOperatorBinding binding,
            RelDataType returnRelDataType) {
        this(dataTypeFactory, definition, binding, returnRelDataType, null);
    }

    public OperatorBindingCallContext(
            DataTypeFactory dataTypeFactory,
            FunctionDefinition definition,
            SqlOperatorBinding binding,
            RelDataType returnRelDataType,
            @Nullable List<ChangelogMode> inputChangelogModes) {
        super(
                dataTypeFactory,
                definition,
                binding.getOperator().getNameAsId().toString(),
                binding.getGroupCount() > 0);

        this.binding = binding;
        this.argumentDataTypes =
                new AbstractList<>() {
                    @Override
                    public DataType get(int pos) {
                        LogicalType logicalType =
                                FlinkTypeFactory.toLogicalType(binding.getOperandType(pos));
                        return fromLogicalToDataType(logicalType);
                    }

                    @Override
                    public int size() {
                        return binding.getOperandCount();
                    }
                };
        this.outputDataType =
                returnRelDataType != null
                        ? fromLogicalToDataType(toLogicalType(returnRelDataType))
                        : null;
        this.inputChangelogModes = inputChangelogModes;
    }

    @Override
    public boolean isArgumentLiteral(int pos) {
        return binding.isOperandLiteral(pos, false);
    }

    @Override
    public boolean isArgumentNull(int pos) {
        return binding.isOperandNull(pos, false) || isDefault(pos);
    }

    @Override
    public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
        if (isArgumentNull(pos)) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(
                    getLiteralValueAs(
                            new LiteralValueAccessor() {
                                @Override
                                public <R> R getValueAs(Class<R> clazz) {
                                    return binding.getOperandLiteralValue(pos, clazz);
                                }
                            },
                            clazz));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<TableSemantics> getTableSemantics(int pos) {
        final StaticArgument staticArg = getStaticArg(pos);
        if (staticArg == null || !staticArg.is(StaticArgumentTrait.TABLE)) {
            return Optional.empty();
        }
        final RexTableArgCall tableArgCall = getTableArgCall(pos);
        if (tableArgCall == null) {
            return Optional.empty();
        }
        final ChangelogMode changelogMode =
                Optional.ofNullable(inputChangelogModes)
                        .map(m -> m.get(tableArgCall.getInputIndex()))
                        .orElse(null);
        return Optional.of(
                RexBindingTableSemantics.create(
                        argumentDataTypes.get(pos), staticArg, tableArgCall, changelogMode));
    }

    @Override
    public List<DataType> getArgumentDataTypes() {
        return argumentDataTypes;
    }

    @Override
    public Optional<DataType> getOutputDataType() {
        return Optional.ofNullable(outputDataType);
    }

    private boolean isDefault(int pos) {
        if (binding instanceof RexCallBinding) {
            final List<RexNode> operands = ((RexCallBinding) binding).operands();
            final RexNode operand = operands.get(pos);
            return operand.getKind() == SqlKind.DEFAULT;
        }
        return false;
    }

    private @Nullable RexTableArgCall getTableArgCall(int pos) {
        if (binding instanceof RexCallBinding) {
            final List<RexNode> operands = ((RexCallBinding) binding).operands();
            final RexNode operand = operands.get(pos);
            return (RexTableArgCall) operand;
        }
        return null;
    }

    private @Nullable StaticArgument getStaticArg(int pos) {
        final SqlOperator operator = binding.getOperator();
        if (!(operator instanceof BridgingSqlFunction)) {
            return null;
        }
        final BridgingSqlFunction function = (BridgingSqlFunction) operator;
        return function.getTypeInference()
                .getStaticArguments()
                .map(args -> args.get(pos))
                .orElse(null);
    }

    // --------------------------------------------------------------------------------------------
    // TableSemantics
    // --------------------------------------------------------------------------------------------

    private static class RexBindingTableSemantics implements TableSemantics {

        private final DataType dataType;
        private final int[] partitionByColumns;
        private final @Nullable ChangelogMode changelogMode;

        public static RexBindingTableSemantics create(
                DataType tableDataType,
                StaticArgument staticArg,
                RexTableArgCall tableArgCall,
                @Nullable ChangelogMode changelogMode) {
            checkNoOrderBy(tableArgCall);
            return new RexBindingTableSemantics(
                    createDataType(tableDataType, staticArg),
                    tableArgCall.getPartitionKeys(),
                    changelogMode);
        }

        private RexBindingTableSemantics(
                DataType dataType,
                int[] partitionByColumns,
                @Nullable ChangelogMode changelogMode) {
            this.dataType = dataType;
            this.partitionByColumns = partitionByColumns;
            this.changelogMode = changelogMode;
        }

        private static void checkNoOrderBy(RexTableArgCall tableArgCall) {
            if (tableArgCall.getOrderKeys().length > 0) {
                throw new ValidationException("ORDER BY clause is currently not supported.");
            }
        }

        private static DataType createDataType(DataType tableDataType, StaticArgument staticArg) {
            final DataType dataType = staticArg.getDataType().orElse(null);
            if (dataType != null) {
                // Typed table argument
                return dataType;
            }
            // Untyped table arguments
            return tableDataType;
        }

        @Override
        public DataType dataType() {
            return dataType;
        }

        @Override
        public int[] partitionByColumns() {
            return partitionByColumns;
        }

        @Override
        public int[] orderByColumns() {
            return new int[0];
        }

        @Override
        public int timeColumn() {
            return -1;
        }

        @Override
        public List<String> coPartitionArgs() {
            return List.of();
        }

        @Override
        public Optional<ChangelogMode> changelogMode() {
            return Optional.ofNullable(changelogMode);
        }
    }
}
