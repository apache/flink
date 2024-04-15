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

package org.apache.flink.table.planner.operations;

import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.SqlToRexConverter;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter;
import org.apache.flink.table.planner.typeutils.LogicalRelDataTypeConverter;
import org.apache.flink.table.planner.utils.Expander;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** An implementation of {@link SqlNodeConverter.ConvertContext}. */
public class SqlNodeConvertContext implements SqlNodeConverter.ConvertContext {

    private final FlinkPlannerImpl flinkPlanner;
    private final CatalogManager catalogManager;

    public SqlNodeConvertContext(FlinkPlannerImpl flinkPlanner, CatalogManager catalogManager) {
        this.flinkPlanner = flinkPlanner;
        this.catalogManager = catalogManager;
    }

    @Override
    public SqlValidator getSqlValidator() {
        return flinkPlanner.getOrCreateSqlValidator();
    }

    @Override
    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    @Override
    public RelRoot toRelRoot(SqlNode sqlNode) {
        return flinkPlanner.rel(sqlNode);
    }

    @Override
    public RexNode toRexNode(
            SqlNode sqlNode, RelDataType inputRowType, @Nullable DataType outputType) {
        RelDataTypeFactory relDataTypeFactory = getSqlValidator().getTypeFactory();
        SqlDialect sqlDialect = getSqlDialect();
        RelDataType outputRelType =
                outputType == null
                        ? null
                        : LogicalRelDataTypeConverter.toRelDataType(
                                outputType.getLogicalType(), relDataTypeFactory);
        return new SqlToRexConverter(flinkPlanner, sqlDialect, inputRowType, outputRelType)
                .convertToRexNode(sqlNode);
    }

    @Override
    public List<RexNode> reduceRexNodes(List<RexNode> rexNodes) {
        List<RexNode> reducedNodes = new ArrayList<>();
        RelOptCluster relOptCluster = flinkPlanner.cluster();
        Objects.requireNonNull(relOptCluster.getPlanner().getExecutor())
                .reduce(relOptCluster.getRexBuilder(), rexNodes, reducedNodes);
        return reducedNodes;
    }

    @Override
    public String toQuotedSqlString(SqlNode sqlNode) {
        return sqlNode.toSqlString(getSqlDialect()).getSql();
    }

    private SqlDialect getSqlDialect() {
        SqlParser.Config parserConfig = flinkPlanner.config().getParserConfig();
        return
        // The default implementation of SqlDialect suppresses all table hints since
        // CALCITE-4640, so we should use AnsiSqlDialect instead to reserve table hints.
        new AnsiSqlDialect(
                SqlDialect.EMPTY_CONTEXT
                        .withQuotedCasing(parserConfig.unquotedCasing())
                        .withConformance(parserConfig.conformance())
                        .withUnquotedCasing(parserConfig.unquotedCasing())
                        .withIdentifierQuoteString(parserConfig.quoting().string));
    }

    @Override
    public String expandSqlIdentifiers(String originalSql) {
        return Expander.create(flinkPlanner)
                .expanded(originalSql)
                .substitute(this::toQuotedSqlString);
    }
}
