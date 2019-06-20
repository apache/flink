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
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.CalciteConfig$;
import org.apache.flink.table.calcite.FlinkCalciteCatalogReader;
import org.apache.flink.table.calcite.FlinkContextImpl;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkRelOptClusterFactory;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.calcite.FlinkTypeSystem;
import org.apache.flink.table.codegen.ExpressionReducer;
import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.plan.cost.FlinkCostFactory;
import org.apache.flink.table.util.JavaScalaConversionUtil;
import org.apache.flink.table.validate.FunctionCatalog;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Utility class to create {@link org.apache.calcite.tools.RelBuilder} or {@link FrameworkConfig} used to create
 * a corresponding {@link org.apache.calcite.tools.Planner}. It tries to separate static elements in a
 * {@link org.apache.flink.table.api.TableEnvironment} like: root schema, cost factory, type system etc.
 * from a dynamic properties like e.g. default path to look for objects in the schema.
 */
@Internal
public class PlannerContext {
	private final RelDataTypeSystem typeSystem = new FlinkTypeSystem();
	private final FlinkTypeFactory typeFactory = new FlinkTypeFactory(typeSystem);
	private final TableConfig tableConfig;
	private final FunctionCatalog functionCatalog;
	private final FrameworkConfig frameworkConfig;
	private final RelOptCluster cluster;

	public PlannerContext(
			TableConfig tableConfig,
			FunctionCatalog functionCatalog,
			CalciteSchema rootSchema,
			List<RelTraitDef> traitDefs) {
		this.tableConfig = tableConfig;
		this.functionCatalog = functionCatalog;
		this.frameworkConfig = createFrameworkConfig(rootSchema, traitDefs);

		RelOptPlanner planner = new VolcanoPlanner(frameworkConfig.getCostFactory(), frameworkConfig.getContext());
		planner.setExecutor(frameworkConfig.getExecutor());
		for (RelTraitDef traitDef : frameworkConfig.getTraitDefs()) {
			planner.addRelTraitDef(traitDef);
		}
		this.cluster = FlinkRelOptClusterFactory.create(planner, new RexBuilder(typeFactory));
	}

	private FrameworkConfig createFrameworkConfig(CalciteSchema rootSchema, List<RelTraitDef> traitDefs) {
		return Frameworks.newConfigBuilder()
				.defaultSchema(rootSchema.plus())
				.parserConfig(getSqlParserConfig())
				.costFactory(new FlinkCostFactory())
				.typeSystem(typeSystem)
				.sqlToRelConverterConfig(getSqlToRelConverterConfig(getCalciteConfig(tableConfig)))
				.operatorTable(getSqlOperatorTable(getCalciteConfig(tableConfig), functionCatalog))
				// set the executor to evaluate constant expressions
				.executor(new ExpressionReducer(tableConfig, false))
				.context(new FlinkContextImpl(tableConfig))
				.traitDefs(traitDefs)
				.build();
	}

	/** Returns the {@link FlinkTypeFactory} that will be used. */
	public FlinkTypeFactory getTypeFactory() {
		return typeFactory;
	}

	/**
	 * Creates a configured {@link FlinkRelBuilder} for a planning session.
	 *
	 * @param currentCatalog the current default catalog to look for first during planning.
	 * @param currentDatabase the current default database to look for first during planning.
	 * @return configured rel builder
	 */
	public FlinkRelBuilder createRelBuilder(String currentCatalog, String currentDatabase) {
		FlinkCalciteCatalogReader relOptSchema = createCatalogReader(false, currentCatalog, currentDatabase);
		return new FlinkRelBuilder(frameworkConfig.getContext(), cluster, relOptSchema);
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
				frameworkConfig,
				isLenient -> createCatalogReader(isLenient, currentCatalog, currentDatabase),
				typeFactory,
				cluster);
	}

	private FlinkCalciteCatalogReader createCatalogReader(
			boolean lenientCaseSensitivity,
			String currentCatalog,
			String currentDatabase) {
		SqlParser.Config sqlParserConfig = frameworkConfig.getParserConfig();
		final boolean caseSensitive;
		if (lenientCaseSensitivity) {
			caseSensitive = false;
		} else {
			caseSensitive = sqlParserConfig.caseSensitive();
		}

		SqlParser.Config newSqlParserConfig = SqlParser.configBuilder(sqlParserConfig)
				.setCaseSensitive(caseSensitive)
				.build();

		SchemaPlus rootSchema = getRootSchema(frameworkConfig.getDefaultSchema());
		return new FlinkCalciteCatalogReader(
				CalciteSchema.from(rootSchema),
				asList(
						asList(currentCatalog, currentDatabase),
						singletonList(currentCatalog)
				),
				typeFactory,
				CalciteConfig$.MODULE$.connectionConfig(newSqlParserConfig));
	}

	private SchemaPlus getRootSchema(SchemaPlus schema) {
		if (schema.getParentSchema() == null) {
			return schema;
		} else {
			return getRootSchema(schema.getParentSchema());
		}
	}

	private CalciteConfig getCalciteConfig(TableConfig tableConfig) {
		return tableConfig.getCalciteConfig();
	}

	/**
	 * Returns the SQL parser config for this environment including a custom Calcite configuration.
	 */
	private SqlParser.Config getSqlParserConfig() {
		return JavaScalaConversionUtil.toJava(getCalciteConfig(tableConfig).getSqlParserConfig()).orElseGet(
				// we use Java lex because back ticks are easier than double quotes in programming
				// and cases are preserved
				() -> SqlParser
						.configBuilder()
						.setLex(Lex.JAVA)
						.setIdentifierMaxLength(256)
						.build());
	}

	/**
	 * Returns the {@link SqlToRelConverter} config.
	 *
	 * <p>`expand` is set as false, and each sub-query becomes a [[org.apache.calcite.rex.RexSubQuery]].
	 */
	private SqlToRelConverter.Config getSqlToRelConverterConfig(CalciteConfig calciteConfig) {
		return JavaScalaConversionUtil.toJava(calciteConfig.getSqlToRelConverterConfig()).orElseGet(
				() -> SqlToRelConverter.configBuilder()
						.withTrimUnusedFields(false)
						.withConvertTableAccess(false)
						.withInSubQueryThreshold(Integer.MAX_VALUE)
						.withExpand(false)
						.build()
		);
	}

	/**
	 * Returns the operator table for this environment including a custom Calcite configuration.
	 */
	private SqlOperatorTable getSqlOperatorTable(CalciteConfig calciteConfig, FunctionCatalog functionCatalog) {
		return JavaScalaConversionUtil.toJava(calciteConfig.getSqlOperatorTable()).map(operatorTable -> {
					if (calciteConfig.replacesSqlOperatorTable()) {
						return operatorTable;
					} else {
						return ChainedSqlOperatorTable.of(getBuiltinSqlOperatorTable(functionCatalog), operatorTable);
					}
				}
		).orElseGet(() -> getBuiltinSqlOperatorTable(functionCatalog));
	}

	/**
	 * Returns builtin the operator table for this environment.
	 */
	private SqlOperatorTable getBuiltinSqlOperatorTable(FunctionCatalog functionCatalog) {
		return ChainedSqlOperatorTable.of(
				new ListSqlOperatorTable(functionCatalog.sqlFunctions()),
				FlinkSqlOperatorTable.instance());
	}

}
