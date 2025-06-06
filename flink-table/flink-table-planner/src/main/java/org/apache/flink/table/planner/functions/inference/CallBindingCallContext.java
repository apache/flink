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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.ColumnList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nullable;

import java.util.AbstractList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A {@link CallContext} backed by {@link SqlCallBinding}. Compared to {@link
 * OperatorBindingCallContext}, this class is able to reorder arguments.
 */
@Internal
public final class CallBindingCallContext extends AbstractSqlCallContext {

    private final List<SqlNode> adaptedArguments;
    private final List<DataType> argumentDataTypes;
    private final @Nullable DataType outputType;
    private final @Nullable List<StaticArgument> staticArguments;

    public CallBindingCallContext(
            DataTypeFactory dataTypeFactory,
            FunctionDefinition definition,
            SqlCallBinding binding,
            @Nullable RelDataType outputType,
            @Nullable List<StaticArgument> staticArguments) {
        super(
                dataTypeFactory,
                definition,
                binding.getOperator().getNameAsId().toString(),
                binding.getGroupCount() > 0);
        this.adaptedArguments = binding.operands(); // reorders the operands
        this.argumentDataTypes =
                new AbstractList<>() {
                    @Override
                    public DataType get(int pos) {
                        final LogicalType logicalType =
                                FlinkTypeFactory.toLogicalType(binding.getOperandType(pos));
                        return TypeConversions.fromLogicalToDataType(logicalType);
                    }

                    @Override
                    public int size() {
                        return binding.getOperandCount();
                    }
                };
        this.outputType = convertOutputType(binding, outputType);
        this.staticArguments = staticArguments;
    }

    @Override
    public boolean isArgumentLiteral(int pos) {
        final SqlNode sqlNode = adaptedArguments.get(pos);
        // Semantically a descriptor can be considered a literal,
        // however, Calcite represents them as a call
        return SqlUtil.isLiteral(sqlNode, false) || sqlNode.getKind() == SqlKind.DESCRIPTOR;
    }

    @Override
    public boolean isArgumentNull(int pos) {
        final SqlNode sqlNode = adaptedArguments.get(pos);
        // Default values are passed as NULL into functions.
        // We can introduce a dedicated CallContext.isDefault() method in the future if fine-grained
        // information is required.
        return SqlUtil.isNullLiteral(sqlNode, true) || sqlNode.getKind() == SqlKind.DEFAULT;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
        if (isArgumentNull(pos)) {
            return Optional.empty();
        }
        try {
            final SqlNode sqlNode = adaptedArguments.get(pos);
            if (sqlNode.getKind() == SqlKind.DESCRIPTOR && clazz == ColumnList.class) {
                final List<SqlNode> columns = ((SqlCall) sqlNode).getOperandList();
                if (columns.stream()
                        .anyMatch(
                                column ->
                                        !(column instanceof SqlIdentifier)
                                                || !((SqlIdentifier) column).isSimple())) {
                    return Optional.empty();
                }
                return Optional.of((T) convertColumnList(columns));
            }
            final SqlLiteral literal = SqlLiteral.unchain(sqlNode);
            return Optional.ofNullable(getLiteralValueAs(literal::getValueAs, clazz));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<TableSemantics> getTableSemantics(int pos) {
        final StaticArgument staticArg =
                Optional.ofNullable(staticArguments).map(args -> args.get(pos)).orElse(null);
        if (staticArg == null || !staticArg.is(StaticArgumentTrait.TABLE)) {
            return Optional.empty();
        }
        final SqlNode sqlNode = adaptedArguments.get(pos);
        if (!sqlNode.isA(SqlKind.QUERY) && noSetSemantics(sqlNode)) {
            return Optional.empty();
        }
        return Optional.of(
                CallBindingTableSemantics.create(argumentDataTypes.get(pos), staticArg, sqlNode));
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
    // TableSemantics
    // --------------------------------------------------------------------------------------------

    private static class CallBindingTableSemantics implements TableSemantics {

        private final DataType dataType;
        private final int[] partitionByColumns;

        public static CallBindingTableSemantics create(
                DataType tableDataType, StaticArgument staticArg, SqlNode sqlNode) {
            checkNoOrderBy(sqlNode);
            return new CallBindingTableSemantics(
                    createDataType(tableDataType, staticArg),
                    createPartitionByColumns(tableDataType, sqlNode));
        }

        private static void checkNoOrderBy(SqlNode sqlNode) {
            final SqlNodeList orderByList = getSemanticsComponent(sqlNode, 2);
            if (orderByList == null) {
                return;
            }
            if (!orderByList.isEmpty()) {
                throw new ValidationException("ORDER BY clause is currently not supported.");
            }
        }

        private static @Nullable SqlNodeList getSemanticsComponent(SqlNode sqlNode, int pos) {
            if (noSetSemantics(sqlNode)) {
                return null;
            }
            // 0 => query, 1 => PARTITION BY, 2 => ORDER BY
            final List<SqlNode> setSemantics = ((SqlCall) sqlNode).getOperandList();
            return (SqlNodeList) setSemantics.get(pos);
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

        private static int[] createPartitionByColumns(DataType tableDataType, SqlNode sqlNode) {
            final SqlNodeList partitionByList = getSemanticsComponent(sqlNode, 1);
            if (partitionByList == null) {
                return new int[0];
            }
            final List<String> tableColumns = DataType.getFieldNames(tableDataType);
            return partitionByList.stream()
                    .map(n -> ((SqlIdentifier) n).getSimple())
                    .map(
                            c -> {
                                final int pos = tableColumns.indexOf(c);
                                if (pos < 0) {
                                    throw new ValidationException(
                                            String.format(
                                                    "Invalid column '%s' for PARTITION BY clause. "
                                                            + "Available columns are: %s",
                                                    c, tableColumns));
                                }
                                return pos;
                            })
                    .mapToInt(Integer::intValue)
                    .toArray();
        }

        private CallBindingTableSemantics(DataType dataType, int[] partitionByColumns) {
            this.dataType = dataType;
            this.partitionByColumns = partitionByColumns;
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
        public Optional<ChangelogMode> changelogMode() {
            return Optional.empty();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private static @Nullable DataType convertOutputType(
            SqlCallBinding binding, @Nullable RelDataType returnType) {
        if (returnType == null
                || returnType.equals(binding.getValidator().getUnknownType())
                || returnType.getSqlTypeName() == SqlTypeName.ANY) {
            return null;
        } else {
            final LogicalType logicalType = FlinkTypeFactory.toLogicalType(returnType);
            return TypeConversions.fromLogicalToDataType(logicalType);
        }
    }

    private static boolean noSetSemantics(SqlNode sqlNode) {
        return sqlNode.getKind() != SqlKind.SET_SEMANTICS_TABLE;
    }

    private static ColumnList convertColumnList(List<SqlNode> operands) {
        final List<String> names =
                operands.stream()
                        .map(operand -> ((SqlIdentifier) operand).getSimple())
                        .collect(Collectors.toList());
        return ColumnList.of(names);
    }
}
