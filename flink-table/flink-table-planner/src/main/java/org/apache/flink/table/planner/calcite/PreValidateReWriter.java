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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.sql.parser.SqlProperty;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dql.SqlRichExplain;
import org.apache.flink.sql.parser.type.SqlMapTypeNameSpec;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.plan.schema.CatalogSourceTable;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.LegacyCatalogSourceTable;

import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.calcite.util.Static.RESOURCE;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Implements {@link org.apache.calcite.sql.util.SqlVisitor} interface to do some rewrite work
 * before sql node validation.
 */
public class PreValidateReWriter extends SqlBasicVisitor<Void> {
    public final FlinkCalciteSqlValidator validator;
    public final RelDataTypeFactory typeFactory;

    public PreValidateReWriter(FlinkCalciteSqlValidator validator, RelDataTypeFactory typeFactory) {
        this.validator = validator;
        this.typeFactory = typeFactory;
    }

    @Override
    public Void visit(SqlCall call) {
        if (call instanceof SqlRichExplain) {
            SqlNode statement = ((SqlRichExplain) call).getStatement();
            if (statement instanceof RichSqlInsert) {
                rewriteInsert((RichSqlInsert) statement);
            }
            // do nothing
        } else if (call instanceof RichSqlInsert) {
            rewriteInsert((RichSqlInsert) call);
        }
        // do nothing and return 'void'
        return null;
    }

    private void rewriteInsert(RichSqlInsert r) {
        if (r.getStaticPartitions().size() != 0 || r.getTargetColumnList() != null) {
            SqlNode source = r.getSource();
            if (source instanceof SqlCall) {
                SqlCall newSource =
                        appendPartitionAndNullsProjects(
                                r,
                                validator,
                                typeFactory,
                                (SqlCall) source,
                                r.getStaticPartitions());
                r.setOperand(2, newSource);
            } else {
                throw new ValidationException(notSupported(source));
            }
        }
    }

    // ~ Tools ------------------------------------------------------------------

    private static String notSupported(SqlNode source) {
        return String.format(
                "INSERT INTO <table> PARTITION [(COLUMN LIST)] statement only "
                        + "support SELECT, VALUES, SET_QUERY AND ORDER BY clause for now, "
                        + "'%s' is not supported yet.",
                source);
    }

    /**
     * Append the static partitions and unspecified columns to the data source projection list. The
     * columns are appended to the corresponding positions.
     *
     * <p>If we have a table A with schema (&lt;a&gt;, &lt;b&gt;, &lt;c&gt) whose partition columns
     * are (&lt;a&gt;, &lt;c&gt;), and got a query
     *
     * <blockquote>
     *
     * <pre> insert into A partition(a='11',
     * c='22') select b from B </pre>
     *
     * </blockquote>
     *
     * <p>The query would be rewritten to:
     *
     * <blockquote>
     *
     * <pre>
     * insert into A partition(a='11', c='22') select cast('11' as tpe1), b, cast('22' as tpe2) from B
     * </pre>
     *
     * </blockquote>
     *
     * <p>Where the "tpe1" and "tpe2" are data types of column a and c of target table A.
     *
     * <p>If we have a table A with schema (&lt;a&gt;, &lt;b&gt;, &lt;c&gt), and got a query
     *
     * <blockquote>
     *
     * <pre> insert into A (a, b) select a, b from B </pre>
     *
     * </blockquote>
     *
     * <p>The query would be rewritten to:
     *
     * <blockquote>
     *
     * <pre> insert into A select a, b, cast(null as tpeC) from B
     * </pre>
     *
     * </blockquote>
     *
     * <p>Where the "tpeC" is data type of column c for target table A.
     *
     * @param sqlInsert RichSqlInsert instance
     * @param validator Validator
     * @param typeFactory type factory
     * @param source Source to rewrite
     * @param partitions Static partition statements
     */
    public static SqlCall appendPartitionAndNullsProjects(
            RichSqlInsert sqlInsert,
            FlinkCalciteSqlValidator validator,
            RelDataTypeFactory typeFactory,
            SqlCall source,
            SqlNodeList partitions) {
        CalciteCatalogReader calciteCatalogReader =
                validator.getCatalogReader().unwrap(CalciteCatalogReader.class);
        List<String> names = ((SqlIdentifier) sqlInsert.getTargetTable()).names;
        Prepare.PreparingTable table = calciteCatalogReader.getTable(names);
        if (table == null) {
            // There is no table exists in current catalog,
            // just skip to let other validation error throw.
            return source;
        }
        RelDataType targetRowType = createTargetRowType(typeFactory, table);
        // validate partition fields first.
        LinkedHashMap<Integer, SqlNode> assignedFields = new LinkedHashMap<>();
        for (SqlNode node : partitions.getList()) {
            SqlProperty sqlProperty = (SqlProperty) node;
            SqlIdentifier id = sqlProperty.getKey();
            RelDataTypeField targetField =
                    SqlValidatorUtil.getTargetField(
                            targetRowType, typeFactory, id, calciteCatalogReader, table);
            validateField(idx -> !assignedFields.containsKey(idx), id, targetField);
            SqlLiteral value = (SqlLiteral) sqlProperty.getValue();
            assignedFields.put(
                    targetField.getIndex(),
                    maybeCast(
                            value,
                            value.createSqlType(typeFactory),
                            targetField.getType(),
                            typeFactory));
        }

        // validate partial insert columns.

        // the columnList may reorder fields (compare with fields of sink)
        List<Integer> targetPosition = new ArrayList<>();
        if (sqlInsert.getTargetColumnList() != null) {
            Set<Integer> targetFields = new HashSet<>();
            List<RelDataTypeField> targetColumns =
                    sqlInsert.getTargetColumnList().getList().stream()
                            .map(
                                    id -> {
                                        RelDataTypeField targetField =
                                                SqlValidatorUtil.getTargetField(
                                                        targetRowType,
                                                        typeFactory,
                                                        (SqlIdentifier) id,
                                                        calciteCatalogReader,
                                                        table);
                                        validateField(
                                                targetFields::add, (SqlIdentifier) id, targetField);
                                        return targetField;
                                    })
                            .collect(Collectors.toList());

            List<RelDataTypeField> partitionColumns =
                    partitions.getList().stream()
                            .map(
                                    property ->
                                            SqlValidatorUtil.getTargetField(
                                                    targetRowType,
                                                    typeFactory,
                                                    ((SqlProperty) property).getKey(),
                                                    calciteCatalogReader,
                                                    table))
                            .collect(Collectors.toList());

            for (RelDataTypeField targetField : targetRowType.getFieldList()) {
                if (!partitionColumns.contains(targetField)) {
                    if (!targetColumns.contains(targetField)) {
                        // padding null
                        SqlIdentifier id =
                                new SqlIdentifier(targetField.getName(), SqlParserPos.ZERO);
                        if (!targetField.getType().isNullable()) {
                            throw newValidationError(
                                    id, RESOURCE.columnNotNullable(targetField.getName()));
                        }
                        validateField(idx -> !assignedFields.containsKey(idx), id, targetField);
                        assignedFields.put(
                                targetField.getIndex(),
                                maybeCast(
                                        SqlLiteral.createNull(SqlParserPos.ZERO),
                                        typeFactory.createUnknownType(),
                                        targetField.getType(),
                                        typeFactory));
                    } else {
                        // handle reorder
                        targetPosition.add(targetColumns.indexOf(targetField));
                    }
                }
            }
        }
        return rewriteSqlCall(validator, source, targetRowType, assignedFields, targetPosition);
    }

    private static SqlCall rewriteSqlCall(
            FlinkCalciteSqlValidator validator,
            SqlCall call,
            RelDataType targetRowType,
            LinkedHashMap<Integer, SqlNode> assignedFields,
            List<Integer> targetPosition) {
        SqlKind kind = call.getKind();
        if (kind == SqlKind.SELECT) {
            return rewriteSelect(
                    validator, (SqlSelect) call, targetRowType, assignedFields, targetPosition);
        } else if (kind == SqlKind.VALUES) {
            return rewriteValues(call, targetRowType, assignedFields, targetPosition);
        } else if (SqlKind.SET_QUERY.contains(kind)) {
            List<SqlNode> operands = call.getOperandList();
            for (int i = 0; i < operands.size(); i++) {
                call.setOperand(
                        i,
                        rewrite(
                                validator,
                                operands.get(i),
                                targetRowType,
                                assignedFields,
                                targetPosition));
            }
            return call;
        } else if (kind == SqlKind.ORDER_BY) {
            List<SqlNode> operands = call.getOperandList();
            return new SqlOrderBy(
                    call.getParserPosition(),
                    rewrite(
                            validator,
                            operands.get(0),
                            targetRowType,
                            assignedFields,
                            targetPosition),
                    (SqlNodeList) operands.get(1),
                    operands.get(2),
                    operands.get(3));
        } else {
            // Not support:
            // case SqlKind.WITH =>
            // case SqlKind.EXPLICIT_TABLE =>
            throw new ValidationException(notSupported(call));
        }
    }

    private static SqlCall rewrite(
            FlinkCalciteSqlValidator validator,
            SqlNode node,
            RelDataType targetRowType,
            LinkedHashMap<Integer, SqlNode> assignedFields,
            List<Integer> targetPosition) {
        checkArgument(node instanceof SqlCall, node);
        return rewriteSqlCall(
                validator, (SqlCall) node, targetRowType, assignedFields, targetPosition);
    }

    private static SqlCall rewriteSelect(
            FlinkCalciteSqlValidator validator,
            SqlSelect select,
            RelDataType targetRowType,
            LinkedHashMap<Integer, SqlNode> assignedFields,
            List<Integer> targetPosition) {
        // Expands the select list first in case there is a star(*).
        // Validates the select first to register the where scope.
        validator.validate(select);
        List<SqlNode> sourceList =
                validator.expandStar(select.getSelectList(), select, false).getList();

        List<SqlNode> fixedNodes = new ArrayList<>();
        List<SqlNode> currentNodes;
        if (targetPosition.isEmpty()) {
            currentNodes = new ArrayList<>(sourceList);
        } else {
            currentNodes = reorder(new ArrayList<>(sourceList), targetPosition);
        }
        for (int i = 0; i < targetRowType.getFieldList().size(); i++) {
            if (assignedFields.containsKey(i)) {
                fixedNodes.add(assignedFields.get(i));
            } else if (currentNodes.size() > 0) {
                fixedNodes.add(currentNodes.remove(0));
            }
        }
        // Although it is error case, we still append the old remaining
        // projection nodes to new projection.
        if (currentNodes.size() > 0) {
            fixedNodes.addAll(currentNodes);
        }
        select.setSelectList(
                new SqlNodeList(fixedNodes, select.getSelectList().getParserPosition()));
        return select;
    }

    private static SqlCall rewriteValues(
            SqlCall values,
            RelDataType targetRowType,
            LinkedHashMap<Integer, SqlNode> assignedFields,
            List<Integer> targetPosition) {
        List<SqlNode> fixedNodes = new ArrayList<>();
        for (int i = 0; i < values.getOperandList().size(); i++) {
            SqlNode value = values.getOperandList().get(i);
            List<SqlNode> valueAsList;
            if (value.getKind() == SqlKind.ROW) {
                valueAsList = ((SqlCall) value).getOperandList();
            } else {
                valueAsList = Collections.singletonList(value);
            }

            List<SqlNode> currentNodes;
            if (targetPosition.isEmpty()) {
                currentNodes = new ArrayList<>(valueAsList);
            } else {
                currentNodes = reorder(new ArrayList<>(valueAsList), targetPosition);
            }
            List<SqlNode> fieldNodes = new ArrayList<>();
            for (int j = 0; j < targetRowType.getFieldList().size(); j++) {
                if (assignedFields.containsKey(j)) {
                    fieldNodes.add(assignedFields.get(j));
                } else if (currentNodes.size() > 0) {
                    fieldNodes.add(currentNodes.remove(0));
                }
            }
            // Although it is error case, we still append the old remaining
            // value items to new item list.
            if (currentNodes.size() > 0) {
                fieldNodes.addAll(currentNodes);
            }
            fixedNodes.add(
                    SqlStdOperatorTable.ROW.createCall(value.getParserPosition(), fieldNodes));
        }
        return SqlStdOperatorTable.VALUES.createCall(values.getParserPosition(), fixedNodes);
    }

    /**
     * Reorder sourceList to targetPosition. For example:
     *
     * <p>sourceList(f0, f1, f2).
     *
     * <p>targetPosition(1, 2, 0).
     *
     * <p>Output(f1, f2, f0).
     *
     * @param sourceList input fields.
     * @param targetPosition reorder mapping.
     * @return reorder fields.
     */
    private static List<SqlNode> reorder(List<SqlNode> sourceList, List<Integer> targetPosition) {
        List<SqlNode> result = new ArrayList<>();
        for (int p : targetPosition) {
            result.add(sourceList.get(p));
        }
        return result;
    }

    /**
     * Derives a physical row-type for INSERT and UPDATE operations.
     *
     * <p>This code snippet is almost inspired by {@code
     * org.apache.calcite.sql.validate.SqlValidatorImpl#createTargetRowType}. It is the best that
     * the logic can be merged into Apache Calcite, but this needs time.
     *
     * @param typeFactory TypeFactory
     * @param table Target table for INSERT/UPDATE
     * @return Rowtype
     */
    private static RelDataType createTargetRowType(
            RelDataTypeFactory typeFactory, SqlValidatorTable table) {
        FlinkPreparingTableBase tableBase = table.unwrap(FlinkPreparingTableBase.class);
        TableSchema schema;
        if (tableBase instanceof CatalogSourceTable) {
            schema = ((CatalogSourceTable) tableBase).getCatalogTable().getSchema();
            return ((FlinkTypeFactory) typeFactory).buildPhysicalRelNodeRowType(schema);
        } else if (tableBase instanceof LegacyCatalogSourceTable) {
            schema = ((LegacyCatalogSourceTable<?>) tableBase).catalogTable().getSchema();
            return ((FlinkTypeFactory) typeFactory).buildPhysicalRelNodeRowType(schema);
        } else {
            return table.getRowType();
        }
    }

    /** Check whether the field is valid. * */
    private static void validateField(
            Function<Integer, Boolean> tester, SqlIdentifier id, RelDataTypeField targetField) {
        if (targetField == null) {
            throw newValidationError(id, RESOURCE.unknownTargetColumn(id.toString()));
        }
        if (!tester.apply(targetField.getIndex())) {
            throw newValidationError(id, RESOURCE.duplicateTargetColumn(targetField.getName()));
        }
    }

    private static CalciteContextException newValidationError(
            SqlNode node, Resources.ExInst<SqlValidatorException> e) {
        assert (node != null);
        SqlParserPos pos = node.getParserPosition();
        return SqlUtil.newContextException(pos, e);
    }

    // This code snippet is copied from the SqlValidatorImpl.
    private static SqlNode maybeCast(
            SqlNode node,
            RelDataType currentType,
            RelDataType desiredType,
            RelDataTypeFactory typeFactory) {
        if (currentType == desiredType
                || (currentType.isNullable() != desiredType.isNullable()
                        && typeFactory.createTypeWithNullability(
                                        currentType, desiredType.isNullable())
                                == desiredType)) {
            return node;
        } else {
            SqlDataTypeSpec sqlDataTypeSpec;
            if (SqlTypeUtil.isNull(currentType) && SqlTypeUtil.isMap(desiredType)) {
                RelDataType keyType = desiredType.getKeyType();
                RelDataType valueType = desiredType.getValueType();
                sqlDataTypeSpec =
                        new SqlDataTypeSpec(
                                new SqlMapTypeNameSpec(
                                        SqlTypeUtil.convertTypeToSpec(keyType)
                                                .withNullable(keyType.isNullable()),
                                        SqlTypeUtil.convertTypeToSpec(valueType)
                                                .withNullable(valueType.isNullable()),
                                        SqlParserPos.ZERO),
                                SqlParserPos.ZERO);
            } else {
                sqlDataTypeSpec = SqlTypeUtil.convertTypeToSpec(desiredType);
            }
            return SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, node, sqlDataTypeSpec);
        }
    }
}
