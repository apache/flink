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
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter;
import org.apache.flink.table.planner.utils.Expander;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;

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
    public String toQuotedSqlString(SqlNode sqlNode) {
        SqlParser.Config parserConfig = flinkPlanner.config().getParserConfig();
        SqlDialect dialect =
                new CalciteSqlDialect(
                        SqlDialect.EMPTY_CONTEXT
                                .withQuotedCasing(parserConfig.unquotedCasing())
                                .withConformance(parserConfig.conformance())
                                .withUnquotedCasing(parserConfig.unquotedCasing())
                                .withIdentifierQuoteString(parserConfig.quoting().string));
        return sqlNode.toSqlString(dialect).getSql();
    }

    @Override
    public String expandSqlIdentifiers(String originalSql) {
        return Expander.create(flinkPlanner)
                .expanded(originalSql)
                .substitute(this::toQuotedSqlString);
    }
}
