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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.calcite.CalciteConfig$;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkContextImpl;
import org.apache.flink.table.planner.calcite.FlinkConvertletTable;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkRelFactories;
import org.apache.flink.table.planner.calcite.FlinkRelOptClusterFactory;
import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.calcite.RexFactory;
import org.apache.flink.table.planner.catalog.FunctionCatalogOperatorTable;
import org.apache.flink.table.planner.codegen.ExpressionReducer;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.hint.FlinkHintStrategies;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;
import org.apache.flink.table.planner.plan.cost.FlinkCostFactory;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.util.StringUtils;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Utility class to create {@link org.apache.calcite.tools.RelBuilder} or {@link FrameworkConfig}
 * used to create a corresponding {@link org.apache.calcite.tools.Planner}. It tries to separate
 * static elements in a {@link org.apache.flink.table.api.TableEnvironment} like: root schema, cost
 * factory, type system etc. from a dynamic properties like e.g. default path to look for objects in
 * the schema.
 */
@Internal
public class PlannerContext {

    private final RelDataTypeSystem typeSystem;
    private final FlinkTypeFactory typeFactory;
    private final RelOptCluster cluster;
    private final FlinkContext context;
    private final CalciteSchema rootSchema;
    private final List<RelTraitDef> traitDefs;
    private final FrameworkConfig frameworkConfig;

    public PlannerContext(
            boolean isBatchMode,
            TableConfig tableConfig,
            ModuleManager moduleManager,
            FunctionCatalog functionCatalog,
            CatalogManager catalogManager,
            CalciteSchema rootSchema,
            List<RelTraitDef> traitDefs,
            ClassLoader classLoader) {
        this.typeSystem = FlinkTypeSystem.INSTANCE;
        this.typeFactory = new FlinkTypeFactory(classLoader, typeSystem);
        this.context =
                new FlinkContextImpl(
                        isBatchMode,
                        tableConfig,
                        moduleManager,
                        functionCatalog,
                        catalogManager,
                        new RexFactory(
                                typeFactory,
                                this::createFlinkPlanner,
                                this::getCalciteSqlDialect,
                                this::createRelBuilder),
                        classLoader);
        this.rootSchema = rootSchema;
        this.traitDefs = traitDefs;
        // Make a framework config to initialize the RelOptCluster instance,
        // caution that we can only use the attributes that can not be overwritten/configured
        // by user.
        this.frameworkConfig = createFrameworkConfig();

        RelOptPlanner planner =
                new VolcanoPlanner(frameworkConfig.getCostFactory(), frameworkConfig.getContext());
        planner.setExecutor(frameworkConfig.getExecutor());
        for (RelTraitDef traitDef : frameworkConfig.getTraitDefs()) {
            planner.addRelTraitDef(traitDef);
        }
        this.cluster = FlinkRelOptClusterFactory.create(planner, new FlinkRexBuilder(typeFactory));
    }

    public RexFactory getRexFactory() {
        return context.getRexFactory();
    }

    public FrameworkConfig createFrameworkConfig() {
        return Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.plus())
                .parserConfig(getSqlParserConfig())
                .costFactory(new FlinkCostFactory())
                .typeSystem(typeSystem)
                .convertletTable(FlinkConvertletTable.INSTANCE)
                .sqlToRelConverterConfig(getSqlToRelConverterConfig())
                .operatorTable(getSqlOperatorTable(getCalciteConfig()))
                // set the executor to evaluate constant expressions
                .executor(
                        new ExpressionReducer(
                                context.getTableConfig(), context.getClassLoader(), false))
                .context(context)
                .traitDefs(traitDefs)
                .build();
    }

    public FlinkTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public FlinkContext getFlinkContext() {
        return context;
    }

    public FlinkRelBuilder createRelBuilder() {
        final FlinkPlannerImpl planner = createFlinkPlanner();
        return createRelBuilder(planner);
    }

    public FlinkPlannerImpl createFlinkPlanner() {
        return new FlinkPlannerImpl(
                createFrameworkConfig(), this::createCatalogReader, typeFactory, cluster);
    }

    public CalciteParser createCalciteParser() {
        return new CalciteParser(getSqlParserConfig());
    }

    public FlinkCalciteCatalogReader createCatalogReader(boolean lenientCaseSensitivity) {
        final SqlParser.Config sqlParserConfig = getSqlParserConfig();

        final boolean caseSensitive;
        if (lenientCaseSensitivity) {
            caseSensitive = false;
        } else {
            caseSensitive = sqlParserConfig.caseSensitive();
        }

        final SqlParser.Config newSqlParserConfig =
                sqlParserConfig.withCaseSensitive(caseSensitive);

        final SchemaPlus finalRootSchema = getRootSchema(rootSchema.plus());

        final CatalogManager catalogManager = context.getCatalogManager();
        final List<List<String>> paths = new ArrayList<>();
        if (!StringUtils.isNullOrWhitespaceOnly(catalogManager.getCurrentCatalog())) {
            if (!StringUtils.isNullOrWhitespaceOnly(catalogManager.getCurrentDatabase())) {
                paths.add(
                        asList(
                                catalogManager.getCurrentCatalog(),
                                catalogManager.getCurrentDatabase()));
            }
            paths.add(singletonList(catalogManager.getCurrentCatalog()));
        }

        return new FlinkCalciteCatalogReader(
                CalciteSchema.from(finalRootSchema),
                paths,
                typeFactory,
                CalciteConfig$.MODULE$.connectionConfig(newSqlParserConfig));
    }

    public RelOptCluster getCluster() {
        return cluster;
    }

    private FlinkRelBuilder createRelBuilder(FlinkPlannerImpl planner) {
        final FlinkCalciteCatalogReader calciteCatalogReader = createCatalogReader(false);

        // Sets up the ViewExpander explicitly for FlinkRelBuilder.
        final Context chain =
                Contexts.of(
                        context,
                        planner.createToRelContext(),
                        FlinkRelBuilder.FLINK_REL_BUILDER_CONFIG);

        return FlinkRelBuilder.of(chain, cluster, calciteCatalogReader);
    }

    private SchemaPlus getRootSchema(SchemaPlus schema) {
        if (schema.getParentSchema() == null) {
            return schema;
        } else {
            return getRootSchema(schema.getParentSchema());
        }
    }

    private CalciteConfig getCalciteConfig() {
        return TableConfigUtils.getCalciteConfig(context.getTableConfig());
    }

    /**
     * Returns the SQL parser config for this environment including a custom Calcite configuration.
     */
    private SqlParser.Config getSqlParserConfig() {
        return JavaScalaConversionUtil.<SqlParser.Config>toJava(
                        getCalciteConfig().getSqlParserConfig())
                .orElseGet(
                        // we use Java lex because back ticks are easier than double quotes in
                        // programming and cases are preserved
                        () -> {
                            SqlConformance conformance = getSqlConformance();
                            return SqlParser.config()
                                    .withParserFactory(FlinkSqlParserFactories.create(conformance))
                                    .withConformance(conformance)
                                    .withLex(Lex.JAVA)
                                    .withIdentifierMaxLength(256);
                        });
    }

    private org.apache.calcite.sql.SqlDialect getCalciteSqlDialect() {
        SqlDialect sqlDialect = context.getTableConfig().getSqlDialect();
        switch (sqlDialect) {
            case HIVE:
                return HiveSqlDialect.DEFAULT;
            case DEFAULT:
                return AnsiSqlDialect.DEFAULT;
            default:
                throw new TableException("Unsupported SQL dialect: " + sqlDialect);
        }
    }

    private FlinkSqlConformance getSqlConformance() {
        SqlDialect sqlDialect = context.getTableConfig().getSqlDialect();
        switch (sqlDialect) {
                // Actually, in Hive dialect, we won't use Calcite parser.
                // So, we can just use Flink's default sql conformance as a placeholder
            case HIVE:
            case DEFAULT:
                return FlinkSqlConformance.DEFAULT;
            default:
                throw new TableException("Unsupported SQL dialect: " + sqlDialect);
        }
    }

    /**
     * Returns the {@link SqlToRelConverter} config.
     *
     * <p>`expand` is set as false, and each sub-query becomes a
     * [[org.apache.calcite.rex.RexSubQuery]].
     */
    private SqlToRelConverter.Config getSqlToRelConverterConfig() {
        return JavaScalaConversionUtil.<SqlToRelConverter.Config>toJava(
                        getCalciteConfig().getSqlToRelConverterConfig())
                .orElseGet(
                        () -> {
                            SqlToRelConverter.Config config =
                                    SqlToRelConverter.config()
                                            .withTrimUnusedFields(false)
                                            .withHintStrategyTable(
                                                    FlinkHintStrategies.createHintStrategyTable())
                                            .withInSubQueryThreshold(Integer.MAX_VALUE)
                                            .withExpand(false)
                                            .withRelBuilderFactory(
                                                    FlinkRelFactories.FLINK_REL_BUILDER());

                            // disable project merge in sql2rel phase, let it done by the optimizer
                            boolean mergeProjectsDuringSqlToRel =
                                    context.getTableConfig()
                                            .getConfiguration()
                                            .getBoolean(
                                                    OptimizerConfigOptions
                                                            .TABLE_OPTIMIZER_SQL2REL_PROJECT_MERGE_ENABLED);
                            if (!mergeProjectsDuringSqlToRel) {
                                config = config.addRelBuilderConfigTransform(c -> c.withBloat(-1));
                            }

                            return config;
                        });
    }

    /** Returns the operator table for this environment including a custom Calcite configuration. */
    private SqlOperatorTable getSqlOperatorTable(CalciteConfig calciteConfig) {
        return JavaScalaConversionUtil.<SqlOperatorTable>toJava(calciteConfig.getSqlOperatorTable())
                .map(
                        operatorTable -> {
                            if (calciteConfig.replacesSqlOperatorTable()) {
                                return operatorTable;
                            } else {
                                return SqlOperatorTables.chain(
                                        getBuiltinSqlOperatorTable(), operatorTable);
                            }
                        })
                .orElseGet(this::getBuiltinSqlOperatorTable);
    }

    /** Returns builtin the operator table and external the operator for this environment. */
    private SqlOperatorTable getBuiltinSqlOperatorTable() {
        return SqlOperatorTables.chain(
                new FunctionCatalogOperatorTable(
                        context.getFunctionCatalog(),
                        context.getCatalogManager().getDataTypeFactory(),
                        typeFactory,
                        context.getRexFactory()),
                FlinkSqlOperatorTable.instance(context.isBatchMode()));
    }
}
