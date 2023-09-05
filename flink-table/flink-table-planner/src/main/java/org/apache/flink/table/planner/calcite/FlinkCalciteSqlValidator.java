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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions.ColumnExpansionStrategy;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.DecimalType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.validate.DelegatingScope;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.IdentifierSnapshotNamespace;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.TimestampString;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.flink.table.expressions.resolver.lookups.FieldReferenceLookup.includeExpandedColumn;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Extends Calcite's {@link SqlValidator} by Flink-specific behavior. */
@Internal
public final class FlinkCalciteSqlValidator extends SqlValidatorImpl {

    // Enables CallContext#getOutputDataType() when validating SQL expressions.
    private SqlNode sqlNodeForExpectedOutputType;
    private RelDataType expectedOutputType;

    private final RelOptCluster relOptCluster;

    private final RelOptTable.ToRelContext toRelContext;

    private final FrameworkConfig frameworkConfig;

    public FlinkCalciteSqlValidator(
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            SqlValidator.Config config,
            RelOptTable.ToRelContext toRelcontext,
            RelOptCluster relOptCluster,
            FrameworkConfig frameworkConfig) {
        super(opTab, catalogReader, typeFactory, config);
        this.relOptCluster = relOptCluster;
        this.toRelContext = toRelcontext;
        this.frameworkConfig = frameworkConfig;
    }

    public void setExpectedOutputType(SqlNode sqlNode, RelDataType expectedOutputType) {
        this.sqlNodeForExpectedOutputType = sqlNode;
        this.expectedOutputType = expectedOutputType;
    }

    public Optional<RelDataType> getExpectedOutputType(SqlNode sqlNode) {
        if (sqlNode == sqlNodeForExpectedOutputType) {
            return Optional.of(expectedOutputType);
        }
        return Optional.empty();
    }

    @Override
    public void validateLiteral(SqlLiteral literal) {
        if (literal.getTypeName() == DECIMAL) {
            final BigDecimal decimal = literal.getValueAs(BigDecimal.class);
            if (decimal.precision() > DecimalType.MAX_PRECISION) {
                throw newValidationError(
                        literal, Static.RESOURCE.numberLiteralOutOfRange(decimal.toString()));
            }
        }
        super.validateLiteral(literal);
    }

    @Override
    protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
        // Due to the improper translation of lateral table left outer join in Calcite, we need to
        // temporarily forbid the common predicates until the problem is fixed (see FLINK-7865).
        if (join.getJoinType() == JoinType.LEFT
                && SqlUtil.stripAs(join.getRight()).getKind() == SqlKind.COLLECTION_TABLE) {
            SqlNode right = SqlUtil.stripAs(join.getRight());
            if (right instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) right;
                SqlNode operand0 = call.operand(0);
                if (operand0 instanceof SqlBasicCall
                        && ((SqlBasicCall) operand0).getOperator()
                                instanceof SqlWindowTableFunction) {
                    return;
                }
            }
            final SqlNode condition = join.getCondition();
            if (condition != null
                    && (!SqlUtil.isLiteral(condition)
                            || ((SqlLiteral) condition).getValueAs(Boolean.class)
                                    != Boolean.TRUE)) {
                throw new ValidationException(
                        String.format(
                                "Left outer joins with a table function do not accept a predicate such as %s. "
                                        + "Only literal TRUE is accepted.",
                                condition));
            }
        }
        super.validateJoin(join, scope);
    }

    @Override
    public void validateColumnListParams(
            SqlFunction function, List<RelDataType> argTypes, List<SqlNode> operands) {
        // we don't support column lists and translate them into the unknown type in the type
        // factory,
        // this makes it possible to ignore them in the validator and fall back to regular row types
        // see also SqlFunction#deriveType
    }

    @Override
    protected void registerNamespace(
            @Nullable SqlValidatorScope usingScope,
            @Nullable String alias,
            SqlValidatorNamespace ns,
            boolean forceNullable) {

        // Generate a new validator namespace for time travel scenario.
        // Since time travel only supports constant expressions, we need to ensure that the period
        // of
        // snapshot is not an identifier.
        Optional<SqlSnapshot> snapshot = getSnapShotNode(ns);
        if (usingScope != null
                && snapshot.isPresent()
                && !(snapshot.get().getPeriod() instanceof SqlIdentifier)) {
            SqlSnapshot sqlSnapshot = snapshot.get();
            SqlNode periodNode = sqlSnapshot.getPeriod();
            SqlToRelConverter sqlToRelConverter = this.createSqlToRelConverter();
            RexNode rexNode = sqlToRelConverter.convertExpression(periodNode);
            RexNode simplifiedRexNode =
                    FlinkRexUtil.simplify(
                            sqlToRelConverter.getRexBuilder(),
                            rexNode,
                            relOptCluster.getPlanner().getExecutor());
            List<RexNode> reducedNodes = new ArrayList<>();
            relOptCluster
                    .getPlanner()
                    .getExecutor()
                    .reduce(
                            relOptCluster.getRexBuilder(),
                            Collections.singletonList(simplifiedRexNode),
                            reducedNodes);
            // check whether period is the unsupported expression
            if (!(reducedNodes.get(0) instanceof RexLiteral)) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported time travel expression: %s for the expression can not be reduced to a constant by Flink.",
                                periodNode));
            }

            RexLiteral rexLiteral = (RexLiteral) (reducedNodes).get(0);
            TimestampString timestampString = rexLiteral.getValueAs(TimestampString.class);
            checkNotNull(
                    timestampString,
                    "The time travel expression %s can not reduce to a valid timestamp string. This is a bug. Please file an issue.",
                    periodNode);

            TableConfig tableConfig = ShortcutUtils.unwrapContext(relOptCluster).getTableConfig();
            ZoneId zoneId = tableConfig.getLocalTimeZone();
            long timeTravelTimestamp =
                    TimestampData.fromEpochMillis(timestampString.getMillisSinceEpoch())
                            .toLocalDateTime()
                            .atZone(zoneId)
                            .toInstant()
                            .toEpochMilli();

            SchemaVersion schemaVersion = TimestampSchemaVersion.of(timeTravelTimestamp);
            IdentifierNamespace identifierNamespace = (IdentifierNamespace) ns;
            ns =
                    new IdentifierSnapshotNamespace(
                            identifierNamespace,
                            schemaVersion,
                            ((DelegatingScope) usingScope).getParent());

            sqlSnapshot.setOperand(
                    1,
                    SqlLiteral.createTimestamp(
                            timestampString,
                            rexLiteral.getType().getPrecision(),
                            sqlSnapshot.getPeriod().getParserPosition()));
        }

        super.registerNamespace(usingScope, alias, ns, forceNullable);
    }

    /**
     * Get the {@link SqlSnapshot} node in a {@link SqlValidatorNamespace}.
     *
     * <p>In general, if there is a snapshot expression, the enclosing node of IdentifierNamespace
     * is usually SqlSnapshot. However, if we encounter a situation with an "as" operator, we need
     * to identify whether the enclosingNode is an "as" call and if its first operand is
     * SqlSnapshot.
     *
     * @param ns The namespace used to find SqlSnapshot
     * @return SqlSnapshot found in {@param ns}, empty if not found
     */
    private Optional<SqlSnapshot> getSnapShotNode(SqlValidatorNamespace ns) {
        if (ns instanceof IdentifierNamespace) {
            SqlNode enclosingNode = ns.getEnclosingNode();
            // FOR SYSTEM_TIME AS OF [expression]
            if (enclosingNode instanceof SqlSnapshot) {
                return Optional.of((SqlSnapshot) enclosingNode);
                // FOR SYSTEM_TIME AS OF [expression] as [identifier]
            } else if (enclosingNode instanceof SqlBasicCall
                    && ((SqlBasicCall) enclosingNode).getOperator() instanceof SqlAsOperator
                    && ((SqlBasicCall) enclosingNode).getOperandList().get(0)
                            instanceof SqlSnapshot) {
                return Optional.of(
                        (SqlSnapshot) ((SqlBasicCall) enclosingNode).getOperandList().get(0));
            }
        }
        return Optional.empty();
    }

    private SqlToRelConverter createSqlToRelConverter() {
        return new SqlToRelConverter(
                toRelContext,
                this,
                this.getCatalogReader().unwrap(CalciteCatalogReader.class),
                relOptCluster,
                frameworkConfig.getConvertletTable(),
                frameworkConfig.getSqlToRelConverterConfig());
    }

    @Override
    protected void addToSelectList(
            List<SqlNode> list,
            Set<String> aliases,
            List<Map.Entry<String, RelDataType>> fieldList,
            SqlNode exp,
            SelectScope scope,
            boolean includeSystemVars) {
        final List<ColumnExpansionStrategy> strategies =
                ShortcutUtils.unwrapTableConfig(relOptCluster)
                        .get(TableConfigOptions.TABLE_COLUMN_EXPANSION_STRATEGY);
        // Extract column's origin to apply strategy
        if (!strategies.isEmpty() && exp instanceof SqlIdentifier) {
            final SqlQualified qualified = scope.fullyQualify((SqlIdentifier) exp);
            if (qualified.namespace != null && qualified.namespace.getTable() != null) {
                final CatalogSchemaTable schemaTable =
                        (CatalogSchemaTable) qualified.namespace.getTable().table();
                final ResolvedSchema resolvedSchema =
                        schemaTable.getContextResolvedTable().getResolvedSchema();
                final String columnName = qualified.suffix().get(0);
                final Column column = resolvedSchema.getColumn(columnName).orElse(null);
                if (qualified.suffix().size() == 1 && column != null) {
                    if (includeExpandedColumn(column, strategies)) {
                        super.addToSelectList(
                                list, aliases, fieldList, exp, scope, includeSystemVars);
                    }
                    return;
                }
            }
        }

        // Always add to list
        super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
    }
}
