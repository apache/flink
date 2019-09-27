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

package org.apache.flink.table.planner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkRelBuilderFactory;
import org.apache.flink.table.calcite.FlinkRelOptClusterFactory;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.calcite.FlinkTypeSystem;
import org.apache.flink.table.catalog.BasicOperatorTable;
import org.apache.flink.table.catalog.CatalogReader;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.FunctionCatalogOperatorTable;
import org.apache.flink.table.codegen.ExpressionReducer;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.plan.cost.DataSetCostFactory;
import org.apache.flink.table.util.JavaScalaConversionUtil;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Utility class to create {@link org.apache.calcite.tools.RelBuilder} or {@link FrameworkConfig} used to create
 * a corresponding {@link org.apache.calcite.tools.Planner}. It tries to separate static elements in a
 * {@link org.apache.flink.table.api.TableEnvironment} like: root schema, cost factory, type system etc.
 * from a dynamic properties like e.g. default path to look for objects in the schema.
 */
@Internal
public class PlanningConfigurationBuilder {
	private final RelOptCostFactory costFactory = new DataSetCostFactory();
	private final RelDataTypeSystem typeSystem = new FlinkTypeSystem();
	private final FlinkTypeFactory typeFactory = new FlinkTypeFactory(typeSystem);
	private final RelOptPlanner planner;
	private final ExpressionBridge<PlannerExpression> expressionBridge;
	private final Context context;
	private final TableConfig tableConfig;
	private final FunctionCatalog functionCatalog;
	private CalciteSchema rootSchema;

	public PlanningConfigurationBuilder(
			TableConfig tableConfig,
			FunctionCatalog functionCatalog,
			CalciteSchema rootSchema,
			ExpressionBridge<PlannerExpression> expressionBridge) {
		this.tableConfig = tableConfig;
		this.functionCatalog = functionCatalog;

		// the converter is needed when calling temporal table functions from SQL, because
		// they reference a history table represented with a tree of table operations
		this.context = Contexts.of(expressionBridge);

		this.planner = new VolcanoPlanner(costFactory, context);
		planner.setExecutor(new ExpressionReducer(tableConfig));
		planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

		this.expressionBridge = expressionBridge;

		this.rootSchema = rootSchema;
	}

	/**
	 * Creates a configured {@link FlinkRelBuilder} for a planning session.
	 *
	 * @param currentCatalog the current default catalog to look for first during planning.
	 * @param currentDatabase the current default database to look for first during planning.
	 * @return configured rel builder
	 */
	public FlinkRelBuilder createRelBuilder(String currentCatalog, String currentDatabase) {
		RelOptCluster cluster = FlinkRelOptClusterFactory.create(
			planner,
			new RexBuilder(typeFactory));
		RelOptSchema relOptSchema = createCatalogReader(false, currentCatalog, currentDatabase);

		return new FlinkRelBuilder(context, cluster, relOptSchema, expressionBridge);
	}

	/**
	 * Creates a configured {@link FlinkPlannerImpl} for a planning session.
	 *
	 * @param currentCatalog the current default catalog to look for first during planning.
	 * @param currentDatabase the current default database to look for first during planning.
	 * @return configured flink planner
	 */
	public FlinkPlannerImpl createFlinkPlanner(String currentCatalog, String currentDatabase) {
		return new FlinkPlannerImpl(
			createFrameworkConfig(),
			isLenient -> createCatalogReader(isLenient, currentCatalog, currentDatabase),
			planner,
			typeFactory);
	}

	/** Returns the Calcite {@link org.apache.calcite.plan.RelOptPlanner} that will be used. */
	public RelOptPlanner getPlanner() {
		return planner;
	}

	/** Returns the {@link FlinkTypeFactory} that will be used. */
	public FlinkTypeFactory getTypeFactory() {
		return typeFactory;
	}

	public Context getContext() {
		return context;
	}

	/**
	 * Returns the SQL parser config for this environment including a custom Calcite configuration.
	 */
	public SqlParser.Config getSqlParserConfig() {
		return JavaScalaConversionUtil.toJava(calciteConfig(tableConfig).sqlParserConfig()).orElseGet(() ->
			// we use Java lex because back ticks are easier than double quotes in programming
			// and cases are preserved
			SqlParser
				.configBuilder()
				.setParserFactory(FlinkSqlParserImpl.FACTORY)
				.setConformance(getSqlConformance())
				.setLex(Lex.JAVA)
				.build());
	}

	private FlinkSqlConformance getSqlConformance() {
		SqlDialect sqlDialect = tableConfig.getSqlDialect();
		switch (sqlDialect) {
			case HIVE:
				return FlinkSqlConformance.HIVE;
			case DEFAULT:
				return FlinkSqlConformance.DEFAULT;
			default:
				throw new TableException("Unsupported SQL dialect: " + sqlDialect);
		}
	}

	private CatalogReader createCatalogReader(
			boolean lenientCaseSensitivity,
			String currentCatalog,
			String currentDatabase) {
		SqlParser.Config sqlParserConfig = getSqlParserConfig();
		final boolean caseSensitive;
		if (lenientCaseSensitivity) {
			caseSensitive = false;
		} else {
			caseSensitive = sqlParserConfig.caseSensitive();
		}

		SqlParser.Config parserConfig = SqlParser.configBuilder(sqlParserConfig)
			.setCaseSensitive(caseSensitive)
			.build();

		return new CatalogReader(
			rootSchema,
			asList(
				asList(currentCatalog, currentDatabase),
				singletonList(currentCatalog)
			),
			typeFactory,
			CalciteConfig.connectionConfig(parserConfig));
	}

	private FrameworkConfig createFrameworkConfig() {
		return Frameworks
			.newConfigBuilder()
			.parserConfig(getSqlParserConfig())
			.costFactory(costFactory)
			.typeSystem(typeSystem)
			.operatorTable(getSqlOperatorTable(calciteConfig(tableConfig), functionCatalog))
			.sqlToRelConverterConfig(
				getSqlToRelConverterConfig(
					calciteConfig(tableConfig),
					expressionBridge))
			// set the executor to evaluate constant expressions
			.executor(new ExpressionReducer(tableConfig))
			.build();
	}

	private CalciteConfig calciteConfig(TableConfig tableConfig) {
		return tableConfig.getPlannerConfig()
			.unwrap(CalciteConfig.class)
			.orElseGet(CalciteConfig::DEFAULT);
	}

	/**
	 * Returns the {@link SqlToRelConverter} config.
	 */
	private SqlToRelConverter.Config getSqlToRelConverterConfig(
			CalciteConfig calciteConfig,
			ExpressionBridge<PlannerExpression> expressionBridge) {
		return JavaScalaConversionUtil.toJava(calciteConfig.sqlToRelConverterConfig()).orElseGet(
			() -> SqlToRelConverter.configBuilder()
				.withTrimUnusedFields(false)
				.withConvertTableAccess(false)
				.withInSubQueryThreshold(Integer.MAX_VALUE)
				.withRelBuilderFactory(new FlinkRelBuilderFactory(expressionBridge))
				.build()
		);
	}

	/**
	 * Returns the operator table for this environment including a custom Calcite configuration.
	 */
	private SqlOperatorTable getSqlOperatorTable(CalciteConfig calciteConfig, FunctionCatalog functionCatalog) {
		SqlOperatorTable baseOperatorTable = ChainedSqlOperatorTable.of(
			new BasicOperatorTable(),
			new FunctionCatalogOperatorTable(functionCatalog, typeFactory)
		);

		return JavaScalaConversionUtil.toJava(calciteConfig.sqlOperatorTable()).map(operatorTable -> {
				if (calciteConfig.replacesSqlOperatorTable()) {
					return operatorTable;
				} else {
					return ChainedSqlOperatorTable.of(baseOperatorTable, operatorTable);
				}
			}
		).orElse(baseOperatorTable);
	}
}
