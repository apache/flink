/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.operations;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.dql.SqlRichExplain;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableColumn.ComputedColumn;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.factories.TestManagedTableFactory;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.EndStatementSetOperation;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.LoadModuleOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.ShowFunctionsOperation;
import org.apache.flink.table.operations.ShowFunctionsOperation.FunctionScope;
import org.apache.flink.table.operations.ShowModulesOperation;
import org.apache.flink.table.operations.ShowTablesOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.operations.SourceQueryOperation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.operations.UnloadModuleOperation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.UseModulesOperation;
import org.apache.flink.table.operations.command.AddJarOperation;
import org.apache.flink.table.operations.command.ClearOperation;
import org.apache.flink.table.operations.command.HelpOperation;
import org.apache.flink.table.operations.command.QuitOperation;
import org.apache.flink.table.operations.command.RemoveJarOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.operations.command.ShowJarsOperation;
import org.apache.flink.table.operations.ddl.AlterDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterTableAddConstraintOperation;
import org.apache.flink.table.operations.ddl.AlterTableDropConstraintOperation;
import org.apache.flink.table.operations.ddl.AlterTableOptionsOperation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;
import org.apache.flink.table.operations.ddl.CreateDatabaseOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.operations.ddl.DropDatabaseOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.expressions.utils.Func0$;
import org.apache.flink.table.planner.expressions.utils.Func1$;
import org.apache.flink.table.planner.expressions.utils.Func8$;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.parse.ExtendedParser;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.table.utils.ExpressionResolverMocks;

import org.apache.calcite.sql.SqlNode;
import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;
import static org.apache.flink.table.planner.utils.OperationMatchers.entry;
import static org.apache.flink.table.planner.utils.OperationMatchers.isCreateTableOperation;
import static org.apache.flink.table.planner.utils.OperationMatchers.partitionedBy;
import static org.apache.flink.table.planner.utils.OperationMatchers.withOptions;
import static org.apache.flink.table.planner.utils.OperationMatchers.withSchema;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

/** Test cases for {@link SqlToOperationConverter}. */
public class SqlToOperationConverterTest {
    private final boolean isStreamingMode = false;
    private final TableConfig tableConfig = new TableConfig();
    private final Catalog catalog = new GenericInMemoryCatalog("MockCatalog", "default");
    private final CatalogManager catalogManager =
            CatalogManagerMocks.preparedCatalogManager()
                    .defaultCatalog("builtin", catalog)
                    .config(
                            Configuration.fromMap(
                                    Collections.singletonMap(
                                            ExecutionOptions.RUNTIME_MODE.key(),
                                            RuntimeExecutionMode.BATCH.name())))
                    .build();
    private final ModuleManager moduleManager = new ModuleManager();
    private final FunctionCatalog functionCatalog =
            new FunctionCatalog(tableConfig, catalogManager, moduleManager);
    private final Supplier<FlinkPlannerImpl> plannerSupplier =
            () ->
                    getPlannerContext()
                            .createFlinkPlanner(
                                    catalogManager.getCurrentCatalog(),
                                    catalogManager.getCurrentDatabase());
    private final PlannerContext plannerContext =
            new PlannerContext(
                    true,
                    tableConfig,
                    moduleManager,
                    functionCatalog,
                    catalogManager,
                    asRootSchema(new CatalogManagerCalciteSchema(catalogManager, isStreamingMode)),
                    Collections.emptyList());

    private final Parser parser =
            new ParserImpl(
                    catalogManager,
                    plannerSupplier,
                    () -> plannerSupplier.get().parser(),
                    getPlannerContext().getSqlExprToRexConverterFactory());

    private PlannerContext getPlannerContext() {
        return plannerContext;
    }

    @BeforeEach
    public void before() throws TableAlreadyExistException, DatabaseNotExistException {
        catalogManager.initSchemaResolver(
                isStreamingMode,
                ExpressionResolverMocks.basicResolver(catalogManager, functionCatalog, parser));

        final ObjectPath path1 = new ObjectPath(catalogManager.getCurrentDatabase(), "t1");
        final ObjectPath path2 = new ObjectPath(catalogManager.getCurrentDatabase(), "t2");
        final TableSchema tableSchema =
                TableSchema.builder()
                        .field("a", DataTypes.BIGINT())
                        .field("b", DataTypes.VARCHAR(Integer.MAX_VALUE))
                        .field("c", DataTypes.INT())
                        .field("d", DataTypes.VARCHAR(Integer.MAX_VALUE))
                        .build();
        Map<String, String> options = new HashMap<>();
        options.put("connector", "COLLECTION");
        final CatalogTable catalogTable = new CatalogTableImpl(tableSchema, options, "");
        catalog.createTable(path1, catalogTable, true);
        catalog.createTable(path2, catalogTable, true);
    }

    @AfterEach
    public void after() throws TableNotExistException {
        final ObjectPath path1 = new ObjectPath(catalogManager.getCurrentDatabase(), "t1");
        final ObjectPath path2 = new ObjectPath(catalogManager.getCurrentDatabase(), "t2");
        catalog.dropTable(path1, true);
        catalog.dropTable(path2, true);
    }

    @Test
    public void testUseCatalog() {
        final String sql = "USE CATALOG cat1";
        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(UseCatalogOperation.class);
        assertThat(((UseCatalogOperation) operation).getCatalogName()).isEqualTo("cat1");
        assertThat(operation.asSummaryString()).isEqualTo("USE CATALOG cat1");
    }

    @Test
    public void testUseDatabase() {
        final String sql1 = "USE db1";
        Operation operation1 = parse(sql1, SqlDialect.DEFAULT);
        assertThat(operation1).isInstanceOf(UseDatabaseOperation.class);
        assertThat(((UseDatabaseOperation) operation1).getCatalogName()).isEqualTo("builtin");
        assertThat(((UseDatabaseOperation) operation1).getDatabaseName()).isEqualTo("db1");

        final String sql2 = "USE cat1.db1";
        Operation operation2 = parse(sql2, SqlDialect.DEFAULT);
        assertThat(operation2).isInstanceOf(UseDatabaseOperation.class);
        assertThat(((UseDatabaseOperation) operation2).getCatalogName()).isEqualTo("cat1");
        assertThat(((UseDatabaseOperation) operation2).getDatabaseName()).isEqualTo("db1");
    }

    @Test
    public void testUseDatabaseWithException() {
        final String sql = "USE cat1.db1.tbl1";
        assertThatThrownBy(() -> parse(sql, SqlDialect.DEFAULT))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    public void testCreateDatabase() {
        final String[] createDatabaseSqls =
                new String[] {
                    "create database db1",
                    "create database if not exists cat1.db1",
                    "create database cat1.db1 comment 'db1_comment'",
                    "create database cat1.db1 comment 'db1_comment' with ('k1' = 'v1', 'K2' = 'V2')"
                };
        final String[] expectedCatalogs = new String[] {"builtin", "cat1", "cat1", "cat1"};
        final String expectedDatabase = "db1";
        final String[] expectedComments = new String[] {null, null, "db1_comment", "db1_comment"};
        final boolean[] expectedIgnoreIfExists = new boolean[] {false, true, false, false};
        Map<String, String> properties = new HashMap<>();
        properties.put("k1", "v1");
        properties.put("K2", "V2");
        final Map[] expectedProperties =
                new Map[] {
                    new HashMap<String, String>(),
                    new HashMap<String, String>(),
                    new HashMap<String, String>(),
                    new HashMap(properties)
                };

        for (int i = 0; i < createDatabaseSqls.length; i++) {
            Operation operation = parse(createDatabaseSqls[i], SqlDialect.DEFAULT);
            assertThat(operation).isInstanceOf(CreateDatabaseOperation.class);
            final CreateDatabaseOperation createDatabaseOperation =
                    (CreateDatabaseOperation) operation;
            assertThat(createDatabaseOperation.getCatalogName()).isEqualTo(expectedCatalogs[i]);
            assertThat(createDatabaseOperation.getDatabaseName()).isEqualTo(expectedDatabase);
            assertThat(createDatabaseOperation.getCatalogDatabase().getComment())
                    .isEqualTo(expectedComments[i]);
            assertThat(createDatabaseOperation.isIgnoreIfExists())
                    .isEqualTo(expectedIgnoreIfExists[i]);
            assertThat(createDatabaseOperation.getCatalogDatabase().getProperties())
                    .isEqualTo(expectedProperties[i]);
        }
    }

    @Test
    public void testDropDatabase() {
        final String[] dropDatabaseSqls =
                new String[] {
                    "drop database db1",
                    "drop database if exists db1",
                    "drop database if exists cat1.db1 CASCADE",
                    "drop database if exists cat1.db1 RESTRICT"
                };
        final String[] expectedCatalogs = new String[] {"builtin", "builtin", "cat1", "cat1"};
        final String expectedDatabase = "db1";
        final boolean[] expectedIfExists = new boolean[] {false, true, true, true};
        final boolean[] expectedIsCascades = new boolean[] {false, false, true, false};

        for (int i = 0; i < dropDatabaseSqls.length; i++) {
            Operation operation = parse(dropDatabaseSqls[i], SqlDialect.DEFAULT);
            assertThat(operation).isInstanceOf(DropDatabaseOperation.class);
            final DropDatabaseOperation dropDatabaseOperation = (DropDatabaseOperation) operation;
            assertThat(dropDatabaseOperation.getCatalogName()).isEqualTo(expectedCatalogs[i]);
            assertThat(dropDatabaseOperation.getDatabaseName()).isEqualTo(expectedDatabase);
            assertThat(dropDatabaseOperation.isIfExists()).isEqualTo(expectedIfExists[i]);
            assertThat(dropDatabaseOperation.isCascade()).isEqualTo(expectedIsCascades[i]);
        }
    }

    @Test
    public void testAlterDatabase() throws Exception {
        catalogManager.registerCatalog("cat1", new GenericInMemoryCatalog("default", "default"));
        catalogManager
                .getCatalog("cat1")
                .get()
                .createDatabase(
                        "db1", new CatalogDatabaseImpl(new HashMap<>(), "db1_comment"), true);
        final String sql = "alter database cat1.db1 set ('k1'='v1', 'K2'='V2')";
        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(AlterDatabaseOperation.class);
        Map<String, String> properties = new HashMap<>();
        properties.put("k1", "v1");
        properties.put("K2", "V2");

        AlterDatabaseOperation alterDatabaseOperation = (AlterDatabaseOperation) operation;
        assertThat(alterDatabaseOperation.getDatabaseName()).isEqualTo("db1");
        assertThat(alterDatabaseOperation.getCatalogName()).isEqualTo("cat1");
        assertThat(alterDatabaseOperation.getCatalogDatabase().getComment())
                .isEqualTo("db1_comment");
        assertThat(alterDatabaseOperation.getCatalogDatabase().getProperties())
                .isEqualTo(properties);
    }

    @Test
    public void testLoadModule() {
        final String sql = "LOAD MODULE dummy WITH ('k1' = 'v1', 'k2' = 'v2')";
        final String expectedModuleName = "dummy";
        final Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("k1", "v1");
        expectedOptions.put("k2", "v2");

        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(LoadModuleOperation.class);
        final LoadModuleOperation loadModuleOperation = (LoadModuleOperation) operation;

        assertThat(loadModuleOperation.getModuleName()).isEqualTo(expectedModuleName);
        assertThat(loadModuleOperation.getOptions()).isEqualTo(expectedOptions);
    }

    @Test
    public void testUnloadModule() {
        final String sql = "UNLOAD MODULE dummy";
        final String expectedModuleName = "dummy";

        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(UnloadModuleOperation.class);

        final UnloadModuleOperation unloadModuleOperation = (UnloadModuleOperation) operation;

        assertThat(unloadModuleOperation.getModuleName()).isEqualTo(expectedModuleName);
    }

    @Test
    public void testUseOneModule() {
        final String sql = "USE MODULES dummy";
        final List<String> expectedModuleNames = Collections.singletonList("dummy");

        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(UseModulesOperation.class);

        final UseModulesOperation useModulesOperation = (UseModulesOperation) operation;

        assertThat(useModulesOperation.getModuleNames()).isEqualTo(expectedModuleNames);
        assertThat(useModulesOperation.asSummaryString()).isEqualTo("USE MODULES: [dummy]");
    }

    @Test
    public void testUseMultipleModules() {
        final String sql = "USE MODULES x, y, z";
        final List<String> expectedModuleNames = Arrays.asList("x", "y", "z");

        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(UseModulesOperation.class);

        final UseModulesOperation useModulesOperation = (UseModulesOperation) operation;

        assertThat(useModulesOperation.getModuleNames()).isEqualTo(expectedModuleNames);
        assertThat(useModulesOperation.asSummaryString()).isEqualTo("USE MODULES: [x, y, z]");
    }

    @Test
    public void testShowModules() {
        final String sql = "SHOW MODULES";
        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(ShowModulesOperation.class);
        final ShowModulesOperation showModulesOperation = (ShowModulesOperation) operation;

        assertThat(showModulesOperation.requireFull()).isFalse();
        assertThat(showModulesOperation.asSummaryString()).isEqualTo("SHOW MODULES");
    }

    @Test
    public void testShowTables() {
        final String sql = "SHOW TABLES from cat1.db1 not like 't%'";
        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assert operation instanceof ShowTablesOperation;

        ShowTablesOperation showTablesOperation = (ShowTablesOperation) operation;
        assertThat(showTablesOperation.getCatalogName()).isEqualTo("cat1");
        assertThat(showTablesOperation.getDatabaseName()).isEqualTo("db1");
        assertThat(showTablesOperation.getPreposition()).isEqualTo("FROM");
        assertThat(showTablesOperation.isUseLike()).isTrue();
        assertThat(showTablesOperation.isNotLike()).isTrue();

        final String sql2 = "SHOW TABLES in db2";
        showTablesOperation = (ShowTablesOperation) parse(sql2, SqlDialect.DEFAULT);
        assertThat(showTablesOperation.getCatalogName()).isEqualTo("builtin");
        assertThat(showTablesOperation.getDatabaseName()).isEqualTo("db2");
        assertThat(showTablesOperation.getPreposition()).isEqualTo("IN");
        assertThat(showTablesOperation.isUseLike()).isFalse();
        assertThat(showTablesOperation.isNotLike()).isFalse();

        final String sql3 = "SHOW TABLES";
        showTablesOperation = (ShowTablesOperation) parse(sql3, SqlDialect.DEFAULT);
        assertThat(showTablesOperation.getCatalogName()).isNull();
        assertThat(showTablesOperation.getDatabaseName()).isNull();
        assertThat(showTablesOperation.getPreposition()).isNull();
    }

    @Test
    public void testShowFullModules() {
        final String sql = "SHOW FULL MODULES";
        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(ShowModulesOperation.class);
        final ShowModulesOperation showModulesOperation = (ShowModulesOperation) operation;

        assertThat(showModulesOperation.requireFull()).isTrue();
        assertThat(showModulesOperation.asSummaryString()).isEqualTo("SHOW FULL MODULES");
    }

    @Test
    public void testShowFunctions() {
        final String sql1 = "SHOW FUNCTIONS";
        assertShowFunctions(sql1, sql1, FunctionScope.ALL);

        final String sql2 = "SHOW USER FUNCTIONS";
        assertShowFunctions(sql2, sql2, FunctionScope.USER);
    }

    @Test
    public void testCreateTable() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  b varchar, \n"
                        + "  c int, \n"
                        + "  d varchar"
                        + ")\n"
                        + "  PARTITIONED BY (a, d)\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(CreateTableOperation.class);
        CreateTableOperation op = (CreateTableOperation) operation;
        CatalogTable catalogTable = op.getCatalogTable();
        assertThat(catalogTable.getPartitionKeys()).hasSameElementsAs(Arrays.asList("a", "d"));
        assertThat(catalogTable.getSchema().getFieldNames())
                .isEqualTo(new String[] {"a", "b", "c", "d"});
        assertThat(catalogTable.getSchema().getFieldDataTypes())
                .isEqualTo(
                        new DataType[] {
                            DataTypes.BIGINT(),
                            DataTypes.VARCHAR(Integer.MAX_VALUE),
                            DataTypes.INT(),
                            DataTypes.VARCHAR(Integer.MAX_VALUE)
                        });
    }

    @Test
    public void testCreateTableWithPrimaryKey() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  b varchar, \n"
                        + "  c int, \n"
                        + "  d varchar, \n"
                        + "  constraint ct1 primary key(a, b) not enforced\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(CreateTableOperation.class);
        CreateTableOperation op = (CreateTableOperation) operation;
        CatalogTable catalogTable = op.getCatalogTable();
        TableSchema tableSchema = catalogTable.getSchema();
        assertThat(
                        tableSchema
                                .getPrimaryKey()
                                .map(UniqueConstraint::asSummaryString)
                                .orElse("fakeVal"))
                .isEqualTo("CONSTRAINT ct1 PRIMARY KEY (a, b)");
        assertThat(tableSchema.getFieldNames()).isEqualTo(new String[] {"a", "b", "c", "d"});
        assertThat(tableSchema.getFieldDataTypes())
                .isEqualTo(
                        new DataType[] {
                            DataTypes.BIGINT().notNull(),
                            DataTypes.STRING().notNull(),
                            DataTypes.INT(),
                            DataTypes.STRING()
                        });
    }

    @Test
    public void testCreateTableWithPrimaryKeyEnforced() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  b varchar, \n"
                        + "  c int, \n"
                        + "  d varchar, \n"
                        +
                        // Default is enforced.
                        "  constraint ct1 primary key(a, b)\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        assertThatThrownBy(() -> parse(sql, planner, parser))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Flink doesn't support ENFORCED mode for PRIMARY KEY "
                                + "constraint. ENFORCED/NOT ENFORCED  controls if the constraint "
                                + "checks are performed on the incoming/outgoing data. "
                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode");
    }

    @Test
    public void testCreateTableWithUniqueKey() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  b varchar, \n"
                        + "  c int, \n"
                        + "  d varchar, \n"
                        + "  constraint ct1 unique (a, b) not enforced\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        assertThatThrownBy(() -> parse(sql, planner, parser))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("UNIQUE constraint is not supported yet");
    }

    @Test
    public void testPrimaryKeyOnGeneratedColumn() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint not null,\n"
                        + "  b varchar not null,\n"
                        + "  c as 2 * (a + 1),\n"
                        + "  constraint ct1 primary key (b, c) not enforced"
                        + ") with (\n"
                        + "    'connector' = 'kafka',\n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Could not create a PRIMARY KEY with column 'c' at line 5, column 34.\n"
                                + "A PRIMARY KEY constraint must be declared on physical columns.");
    }

    @Test
    public void testPrimaryKeyNonExistentColumn() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint not null,\n"
                        + "  b varchar not null,\n"
                        + "  c as 2 * (a + 1),\n"
                        + "  constraint ct1 primary key (b, d) not enforced"
                        + ") with (\n"
                        + "    'connector' = 'kafka',\n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Primary key column 'd' is not defined in the schema at line 5, column 34");
    }

    @Test
    public void testCreateTableWithMinusInOptionKey() {
        final String sql =
                "create table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + "  'a-B-c-d124' = 'Ab',\n"
                        + "  'a.b-c-d.e-f.g' = 'ada',\n"
                        + "  'a.b-c-d.e-f1231.g' = 'ada',\n"
                        + "  'a.b-c-d.*' = 'adad')\n";
        final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        SqlNode node = parser.parse(sql);
        assertThat(node).isInstanceOf(SqlCreateTable.class);
        Operation operation = SqlToOperationConverter.convert(planner, catalogManager, node).get();
        assertThat(operation).isInstanceOf(CreateTableOperation.class);
        CreateTableOperation op = (CreateTableOperation) operation;
        CatalogTable catalogTable = op.getCatalogTable();
        Map<String, String> options =
                catalogTable.getOptions().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, String> sortedProperties = new TreeMap<>(options);
        final String expected =
                "{a-B-c-d124=Ab, "
                        + "a.b-c-d.*=adad, "
                        + "a.b-c-d.e-f.g=ada, "
                        + "a.b-c-d.e-f1231.g=ada}";
        assertThat(sortedProperties.toString()).isEqualTo(expected);
    }

    @Test
    public void testExplainWithSelect() {
        final String sql = "explain select * from t1";
        checkExplainSql(sql);
    }

    @Test
    public void testExplainWithInsert() {
        final String sql = "explain insert into t2 select * from t1";
        checkExplainSql(sql);
    }

    @Test
    public void testExplainWithUnion() {
        final String sql = "explain select * from t1 union select * from t2";
        checkExplainSql(sql);
    }

    @Test
    public void testExplainWithExplainDetails() {
        String sql = "explain changelog_mode, estimated_cost, json_execution_plan select * from t1";
        checkExplainSql(sql);
    }

    @Test
    public void testCreateTableWithWatermark()
            throws FunctionAlreadyExistException, DatabaseNotExistException {
        CatalogFunction cf =
                new CatalogFunctionImpl(JavaUserDefinedScalarFunctions.JavaFunc5.class.getName());
        catalog.createFunction(ObjectPath.fromString("default.myfunc"), cf, true);

        final String sql =
                "create table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c timestamp(3),\n"
                        + "  watermark for `c` as myfunc(c, 1) - interval '5' second\n"
                        + ") with (\n"
                        + "  'connector.type' = 'kafka')\n";
        final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        SqlNode node = parser.parse(sql);
        assertThat(node).isInstanceOf(SqlCreateTable.class);

        Operation operation = SqlToOperationConverter.convert(planner, catalogManager, node).get();
        assertThat(operation).isInstanceOf(CreateTableOperation.class);
        CreateTableOperation op = (CreateTableOperation) operation;
        CatalogTable catalogTable = op.getCatalogTable();
        Map<String, String> properties = catalogTable.toProperties();
        Map<String, String> expected = new HashMap<>();
        expected.put("schema.0.name", "a");
        expected.put("schema.0.data-type", "INT");
        expected.put("schema.1.name", "b");
        expected.put("schema.1.data-type", "BIGINT");
        expected.put("schema.2.name", "c");
        expected.put("schema.2.data-type", "TIMESTAMP(3)");
        expected.put("schema.watermark.0.rowtime", "c");
        expected.put(
                "schema.watermark.0.strategy.expr",
                "`builtin`.`default`.`myfunc`(`c`, 1) - INTERVAL '5' SECOND");
        expected.put("schema.watermark.0.strategy.data-type", "TIMESTAMP(3)");
        expected.put("connector.type", "kafka");
        assertThat(properties).isEqualTo(expected);
    }

    @Test
    public void testBasicCreateTableLike() {
        Map<String, String> sourceProperties = new HashMap<>();
        sourceProperties.put("format.type", "json");
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder()
                                .column("f0", DataTypes.INT().notNull())
                                .column("f1", DataTypes.TIMESTAMP(3))
                                .build(),
                        null,
                        Collections.emptyList(),
                        sourceProperties);

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "sourceTable"), false);

        final String sql =
                "create table derivedTable(\n"
                        + "  a int,\n"
                        + "  watermark for f1 as `f1` - interval '5' second\n"
                        + ")\n"
                        + "PARTITIONED BY (a, f0)\n"
                        + "with (\n"
                        + "  'connector.type' = 'kafka'"
                        + ")\n"
                        + "like sourceTable";
        Operation operation = parseAndConvert(sql);

        assertThat(operation)
                .is(
                        new HamcrestCondition<>(
                                isCreateTableOperation(
                                        withSchema(
                                                Schema.newBuilder()
                                                        .column("f0", DataTypes.INT().notNull())
                                                        .column("f1", DataTypes.TIMESTAMP(3))
                                                        .column("a", DataTypes.INT())
                                                        .watermark(
                                                                "f1", "`f1` - INTERVAL '5' SECOND")
                                                        .build()),
                                        withOptions(
                                                entry("connector.type", "kafka"),
                                                entry("format.type", "json")),
                                        partitionedBy("a", "f0"))));
    }

    @Test
    public void testCreateTableLikeWithFullPath() {
        Map<String, String> sourceProperties = new HashMap<>();
        sourceProperties.put("connector.type", "kafka");
        sourceProperties.put("format.type", "json");
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder()
                                .column("f0", DataTypes.INT().notNull())
                                .column("f1", DataTypes.TIMESTAMP(3))
                                .build(),
                        null,
                        Collections.emptyList(),
                        sourceProperties);
        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "sourceTable"), false);
        final String sql = "create table mytable like `builtin`.`default`.sourceTable";
        Operation operation = parseAndConvert(sql);

        assertThat(operation)
                .is(
                        new HamcrestCondition<>(
                                isCreateTableOperation(
                                        withSchema(
                                                Schema.newBuilder()
                                                        .column("f0", DataTypes.INT().notNull())
                                                        .column("f1", DataTypes.TIMESTAMP(3))
                                                        .build()),
                                        withOptions(
                                                entry("connector.type", "kafka"),
                                                entry("format.type", "json")))));
    }

    @Test
    public void testMergingCreateTableLike() {
        Map<String, String> sourceProperties = new HashMap<>();
        sourceProperties.put("format.type", "json");
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder()
                                .column("f0", DataTypes.INT().notNull())
                                .column("f1", DataTypes.TIMESTAMP(3))
                                .columnByExpression("f2", "`f0` + 12345")
                                .watermark("f1", "`f1` - interval '1' second")
                                .build(),
                        null,
                        Arrays.asList("f0", "f1"),
                        sourceProperties);

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "sourceTable"), false);

        final String sql =
                "create table derivedTable(\n"
                        + "  a int,\n"
                        + "  watermark for f1 as `f1` - interval '5' second\n"
                        + ")\n"
                        + "PARTITIONED BY (a, f0)\n"
                        + "with (\n"
                        + "  'connector.type' = 'kafka'"
                        + ")\n"
                        + "like sourceTable (\n"
                        + "   EXCLUDING GENERATED\n"
                        + "   EXCLUDING PARTITIONS\n"
                        + "   OVERWRITING OPTIONS\n"
                        + "   OVERWRITING WATERMARKS"
                        + ")";
        Operation operation = parseAndConvert(sql);

        assertThat(operation)
                .is(
                        new HamcrestCondition<>(
                                isCreateTableOperation(
                                        withSchema(
                                                Schema.newBuilder()
                                                        .column("f0", DataTypes.INT().notNull())
                                                        .column("f1", DataTypes.TIMESTAMP(3))
                                                        .column("a", DataTypes.INT())
                                                        .watermark(
                                                                "f1", "`f1` - INTERVAL '5' SECOND")
                                                        .build()),
                                        withOptions(
                                                entry("connector.type", "kafka"),
                                                entry("format.type", "json")),
                                        partitionedBy("a", "f0"))));
    }

    @Test
    public void testCreateTableInvalidPartition() {
        final String sql =
                "create table derivedTable(\n" + "  a int\n" + ")\n" + "PARTITIONED BY (f3)";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Partition column 'f3' not defined in the table schema. Available columns: ['a']");
    }

    @Test
    public void testCreateTableLikeInvalidPartition() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder().column("f0", DataTypes.INT().notNull()).build(),
                        null,
                        Collections.emptyList(),
                        Collections.emptyMap());
        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "sourceTable"), false);

        final String sql =
                "create table derivedTable(\n"
                        + "  a int\n"
                        + ")\n"
                        + "PARTITIONED BY (f3)\n"
                        + "like sourceTable";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Partition column 'f3' not defined in the table schema. Available columns: ['f0', 'a']");
    }

    @Test
    public void testCreateTableInvalidWatermark() {
        final String sql =
                "create table derivedTable(\n"
                        + "  a int,\n"
                        + "  watermark for f1 as `f1` - interval '5' second\n"
                        + ")";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The rowtime attribute field 'f1' is not defined in the table schema,"
                                + " at line 3, column 17\n"
                                + "Available fields: ['a']");
    }

    @Test
    public void testCreateTableLikeInvalidWatermark() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder().column("f0", DataTypes.INT().notNull()).build(),
                        null,
                        Collections.emptyList(),
                        Collections.emptyMap());
        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "sourceTable"), false);

        final String sql =
                "create table derivedTable(\n"
                        + "  a int,\n"
                        + "  watermark for f1 as `f1` - interval '5' second\n"
                        + ")\n"
                        + "like sourceTable";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The rowtime attribute field 'f1' is not defined in the table schema,"
                                + " at line 3, column 17\n"
                                + "Available fields: ['f0', 'a']");
    }

    @Test
    public void testCreateTableLikeNestedWatermark() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder()
                                .column("f0", DataTypes.INT().notNull())
                                .column(
                                        "f1",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("tmstmp", DataTypes.TIMESTAMP(3))))
                                .build(),
                        null,
                        Collections.emptyList(),
                        Collections.emptyMap());
        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "sourceTable"), false);

        final String sql =
                "create table derivedTable(\n"
                        + "  a int,\n"
                        + "  watermark for f1.t as f1.t - interval '5' second\n"
                        + ")\n"
                        + "like sourceTable";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The rowtime attribute field 'f1.t' is not defined in the table schema,"
                                + " at line 3, column 20\n"
                                + "Nested field 't' was not found in a composite type:"
                                + " ROW<`tmstmp` TIMESTAMP(3)>.");
    }

    @Test
    public void testSqlInsertWithStaticPartition() {
        final String sql = "insert into t1 partition(a=1) select b, c, d from t2";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(SinkModifyOperation.class);
        SinkModifyOperation sinkModifyOperation = (SinkModifyOperation) operation;
        final Map<String, String> expectedStaticPartitions = new HashMap<>();
        expectedStaticPartitions.put("a", "1");
        assertThat(sinkModifyOperation.getStaticPartitions()).isEqualTo(expectedStaticPartitions);
    }

    @Test
    public void testSqlInsertWithDynamicTableOptions() {
        final String sql =
                "insert into t1 /*+ OPTIONS('k1'='v1', 'k2'='v2') */\n"
                        + "select a, b, c, d from t2";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(SinkModifyOperation.class);
        SinkModifyOperation sinkModifyOperation = (SinkModifyOperation) operation;
        Map<String, String> dynamicOptions = sinkModifyOperation.getDynamicOptions();
        assertThat(dynamicOptions).isNotNull();
        assertThat(dynamicOptions.size()).isEqualTo(2);
        assertThat(dynamicOptions.toString()).isEqualTo("{k1=v1, k2=v2}");
    }

    @Test
    public void testDynamicTableWithInvalidOptions() {
        final String sql = "select * from t1 /*+ OPTIONS('opt1', 'opt2') */";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        assertThatThrownBy(() -> parse(sql, planner, parser))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(
                        "Hint [OPTIONS] only support " + "non empty key value options");
    }

    @Test // TODO: tweak the tests when FLINK-13604 is fixed.
    public void testCreateTableWithFullDataTypes() {
        final List<TestItem> testItems =
                Arrays.asList(
                        createTestItem("CHAR", DataTypes.CHAR(1)),
                        createTestItem("CHAR NOT NULL", DataTypes.CHAR(1).notNull()),
                        createTestItem("CHAR NULL", DataTypes.CHAR(1)),
                        createTestItem("CHAR(33)", DataTypes.CHAR(33)),
                        createTestItem("VARCHAR", DataTypes.STRING()),
                        createTestItem("VARCHAR(33)", DataTypes.VARCHAR(33)),
                        createTestItem("STRING", DataTypes.STRING()),
                        createTestItem("BOOLEAN", DataTypes.BOOLEAN()),
                        createTestItem("BINARY", DataTypes.BINARY(1)),
                        createTestItem("BINARY(33)", DataTypes.BINARY(33)),
                        createTestItem("VARBINARY", DataTypes.BYTES()),
                        createTestItem("VARBINARY(33)", DataTypes.VARBINARY(33)),
                        createTestItem("BYTES", DataTypes.BYTES()),
                        createTestItem("DECIMAL", DataTypes.DECIMAL(10, 0)),
                        createTestItem("DEC", DataTypes.DECIMAL(10, 0)),
                        createTestItem("NUMERIC", DataTypes.DECIMAL(10, 0)),
                        createTestItem("DECIMAL(10)", DataTypes.DECIMAL(10, 0)),
                        createTestItem("DEC(10)", DataTypes.DECIMAL(10, 0)),
                        createTestItem("NUMERIC(10)", DataTypes.DECIMAL(10, 0)),
                        createTestItem("DECIMAL(10, 3)", DataTypes.DECIMAL(10, 3)),
                        createTestItem("DEC(10, 3)", DataTypes.DECIMAL(10, 3)),
                        createTestItem("NUMERIC(10, 3)", DataTypes.DECIMAL(10, 3)),
                        createTestItem("TINYINT", DataTypes.TINYINT()),
                        createTestItem("SMALLINT", DataTypes.SMALLINT()),
                        createTestItem("INTEGER", DataTypes.INT()),
                        createTestItem("INT", DataTypes.INT()),
                        createTestItem("BIGINT", DataTypes.BIGINT()),
                        createTestItem("FLOAT", DataTypes.FLOAT()),
                        createTestItem("DOUBLE", DataTypes.DOUBLE()),
                        createTestItem("DOUBLE PRECISION", DataTypes.DOUBLE()),
                        createTestItem("DATE", DataTypes.DATE()),
                        createTestItem("TIME", DataTypes.TIME()),
                        createTestItem("TIME WITHOUT TIME ZONE", DataTypes.TIME()),
                        // Expect to be TIME(3).
                        createTestItem("TIME(3)", DataTypes.TIME()),
                        // Expect to be TIME(3).
                        createTestItem("TIME(3) WITHOUT TIME ZONE", DataTypes.TIME()),
                        createTestItem("TIMESTAMP", DataTypes.TIMESTAMP(6)),
                        createTestItem("TIMESTAMP WITHOUT TIME ZONE", DataTypes.TIMESTAMP(6)),
                        createTestItem("TIMESTAMP(3)", DataTypes.TIMESTAMP(3)),
                        createTestItem("TIMESTAMP(3) WITHOUT TIME ZONE", DataTypes.TIMESTAMP(3)),
                        createTestItem(
                                "TIMESTAMP WITH LOCAL TIME ZONE",
                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)),
                        createTestItem(
                                "TIMESTAMP(3) WITH LOCAL TIME ZONE",
                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                        createTestItem(
                                "ARRAY<TIMESTAMP(3) WITH LOCAL TIME ZONE>",
                                DataTypes.ARRAY(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))),
                        createTestItem(
                                "ARRAY<INT NOT NULL>", DataTypes.ARRAY(DataTypes.INT().notNull())),
                        createTestItem("INT ARRAY", DataTypes.ARRAY(DataTypes.INT())),
                        createTestItem(
                                "INT NOT NULL ARRAY", DataTypes.ARRAY(DataTypes.INT().notNull())),
                        createTestItem(
                                "INT ARRAY NOT NULL", DataTypes.ARRAY(DataTypes.INT()).notNull()),
                        createTestItem(
                                "MULTISET<INT NOT NULL>",
                                DataTypes.MULTISET(DataTypes.INT().notNull())),
                        createTestItem("INT MULTISET", DataTypes.MULTISET(DataTypes.INT())),
                        createTestItem(
                                "INT NOT NULL MULTISET",
                                DataTypes.MULTISET(DataTypes.INT().notNull())),
                        createTestItem(
                                "INT MULTISET NOT NULL",
                                DataTypes.MULTISET(DataTypes.INT()).notNull()),
                        createTestItem(
                                "MAP<BIGINT, BOOLEAN>",
                                DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BOOLEAN())),
                        // Expect to be ROW<`f0` INT NOT NULL, `f1` BOOLEAN>.
                        createTestItem(
                                "ROW<f0 INT NOT NULL, f1 BOOLEAN>",
                                DataTypes.ROW(
                                        DataTypes.FIELD("f0", DataTypes.INT()),
                                        DataTypes.FIELD("f1", DataTypes.BOOLEAN()))),
                        // Expect to be ROW<`f0` INT NOT NULL, `f1` BOOLEAN>.
                        createTestItem(
                                "ROW(f0 INT NOT NULL, f1 BOOLEAN)",
                                DataTypes.ROW(
                                        DataTypes.FIELD("f0", DataTypes.INT()),
                                        DataTypes.FIELD("f1", DataTypes.BOOLEAN()))),
                        createTestItem(
                                "ROW<`f0` INT>",
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT()))),
                        createTestItem(
                                "ROW(`f0` INT)",
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT()))),
                        createTestItem("ROW<>", DataTypes.ROW()),
                        createTestItem("ROW()", DataTypes.ROW()),
                        // Expect to be ROW<`f0` INT NOT NULL '...', `f1` BOOLEAN '...'>.
                        createTestItem(
                                "ROW<f0 INT NOT NULL 'This is a comment.',"
                                        + " f1 BOOLEAN 'This as well.'>",
                                DataTypes.ROW(
                                        DataTypes.FIELD("f0", DataTypes.INT()),
                                        DataTypes.FIELD("f1", DataTypes.BOOLEAN()))),
                        createTestItem(
                                "ARRAY<ROW<f0 INT, f1 BOOLEAN>>",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("f0", DataTypes.INT()),
                                                DataTypes.FIELD("f1", DataTypes.BOOLEAN())))),
                        createTestItem(
                                "ROW<f0 INT, f1 BOOLEAN> MULTISET",
                                DataTypes.MULTISET(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("f0", DataTypes.INT()),
                                                DataTypes.FIELD("f1", DataTypes.BOOLEAN())))),
                        createTestItem(
                                "MULTISET<ROW<f0 INT, f1 BOOLEAN>>",
                                DataTypes.MULTISET(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("f0", DataTypes.INT()),
                                                DataTypes.FIELD("f1", DataTypes.BOOLEAN())))),
                        createTestItem(
                                "ROW<f0 Row<f00 INT, f01 BOOLEAN>, "
                                        + "f1 INT ARRAY, "
                                        + "f2 BOOLEAN MULTISET>",
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "f0",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("f00", DataTypes.INT()),
                                                        DataTypes.FIELD(
                                                                "f01", DataTypes.BOOLEAN()))),
                                        DataTypes.FIELD("f1", DataTypes.ARRAY(DataTypes.INT())),
                                        DataTypes.FIELD(
                                                "f2", DataTypes.MULTISET(DataTypes.BOOLEAN())))));
        StringBuilder buffer = new StringBuilder("create table t1(\n");
        for (int i = 0; i < testItems.size(); i++) {
            buffer.append("f").append(i).append(" ").append(testItems.get(i).testExpr);
            if (i == testItems.size() - 1) {
                buffer.append(")");
            } else {
                buffer.append(",\n");
            }
        }
        final String sql = buffer.toString();
        final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        SqlNode node = parser.parse(sql);
        assertThat(node).isInstanceOf(SqlCreateTable.class);
        Operation operation = SqlToOperationConverter.convert(planner, catalogManager, node).get();
        TableSchema schema = ((CreateTableOperation) operation).getCatalogTable().getSchema();
        Object[] expectedDataTypes = testItems.stream().map(item -> item.expectedType).toArray();
        assertThat(schema.getFieldDataTypes()).isEqualTo(expectedDataTypes);
    }

    @Test
    public void testCreateTableWithComputedColumn() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a int,\n"
                        + "  b varchar, \n"
                        + "  c as a - 1, \n"
                        + "  d as b || '$$', \n"
                        + "  e as my_udf1(a),"
                        + "  f as `default`.my_udf2(a) + 1,"
                        + "  g as builtin.`default`.my_udf3(a) || '##'\n"
                        + ")\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        functionCatalog.registerTempCatalogScalarFunction(
                ObjectIdentifier.of("builtin", "default", "my_udf1"), Func0$.MODULE$);
        functionCatalog.registerTempCatalogScalarFunction(
                ObjectIdentifier.of("builtin", "default", "my_udf2"), Func1$.MODULE$);
        functionCatalog.registerTempCatalogScalarFunction(
                ObjectIdentifier.of("builtin", "default", "my_udf3"), Func8$.MODULE$);
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, getParserBySqlDialect(SqlDialect.DEFAULT));
        assertThat(operation).isInstanceOf(CreateTableOperation.class);
        CreateTableOperation op = (CreateTableOperation) operation;
        CatalogTable catalogTable = op.getCatalogTable();
        assertThat(catalogTable.getSchema().getFieldNames())
                .isEqualTo(new String[] {"a", "b", "c", "d", "e", "f", "g"});
        assertThat(catalogTable.getSchema().getFieldDataTypes())
                .isEqualTo(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.STRING(),
                            DataTypes.INT().notNull(),
                            DataTypes.INT(),
                            DataTypes.STRING()
                        });
        String[] columnExpressions =
                catalogTable.getSchema().getTableColumns().stream()
                        .filter(ComputedColumn.class::isInstance)
                        .map(ComputedColumn.class::cast)
                        .map(ComputedColumn::getExpression)
                        .toArray(String[]::new);
        String[] expected =
                new String[] {
                    "`a` - 1",
                    "`b` || '$$'",
                    "`builtin`.`default`.`my_udf1`(`a`)",
                    "`builtin`.`default`.`my_udf2`(`a`) + 1",
                    "`builtin`.`default`.`my_udf3`(`a`) || '##'"
                };
        assertThat(columnExpressions).isEqualTo(expected);
    }

    @Test
    public void testCreateTableWithMetadataColumn() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a INT,\n"
                        + "  b STRING,\n"
                        + "  c INT METADATA,\n"
                        + "  d INT METADATA FROM 'other.key',\n"
                        + "  e INT METADATA VIRTUAL\n"
                        + ")\n"
                        + "  WITH (\n"
                        + "    'connector' = 'kafka',\n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";

        final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final Operation operation = parse(sql, planner, getParserBySqlDialect(SqlDialect.DEFAULT));
        assertThat(operation).isInstanceOf(CreateTableOperation.class);
        final CreateTableOperation op = (CreateTableOperation) operation;
        final TableSchema actualSchema = op.getCatalogTable().getSchema();

        final TableSchema expectedSchema =
                TableSchema.builder()
                        .add(TableColumn.physical("a", DataTypes.INT()))
                        .add(TableColumn.physical("b", DataTypes.STRING()))
                        .add(TableColumn.metadata("c", DataTypes.INT()))
                        .add(TableColumn.metadata("d", DataTypes.INT(), "other.key"))
                        .add(TableColumn.metadata("e", DataTypes.INT(), true))
                        .build();

        assertThat(actualSchema).isEqualTo(expectedSchema);
    }

    @Test
    public void testAlterTable() throws Exception {
        prepareNonManagedTable(false);
        final String[] renameTableSqls =
                new String[] {
                    "alter table cat1.db1.tb1 rename to tb2",
                    "alter table db1.tb1 rename to tb2",
                    "alter table tb1 rename to cat1.db1.tb2",
                };
        final ObjectIdentifier expectedIdentifier = ObjectIdentifier.of("cat1", "db1", "tb1");
        final ObjectIdentifier expectedNewIdentifier = ObjectIdentifier.of("cat1", "db1", "tb2");
        // test rename table converter
        for (int i = 0; i < renameTableSqls.length; i++) {
            Operation operation = parse(renameTableSqls[i], SqlDialect.DEFAULT);
            assertThat(operation).isInstanceOf(AlterTableRenameOperation.class);
            final AlterTableRenameOperation alterTableRenameOperation =
                    (AlterTableRenameOperation) operation;
            assertThat(alterTableRenameOperation.getTableIdentifier())
                    .isEqualTo(expectedIdentifier);
            assertThat(alterTableRenameOperation.getNewTableIdentifier())
                    .isEqualTo(expectedNewIdentifier);
        }
        // test alter table options
        Operation operation =
                parse(
                        "alter table cat1.db1.tb1 set ('k1' = 'v1', 'K2' = 'V2')",
                        SqlDialect.DEFAULT);
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("connector", "dummy");
        expectedOptions.put("k", "v");
        expectedOptions.put("k1", "v1");
        expectedOptions.put("K2", "V2");

        assertAlterTableOptions(operation, expectedIdentifier, expectedOptions);

        // test alter table reset
        operation = parse("alter table cat1.db1.tb1 reset ('k')", SqlDialect.DEFAULT);
        assertAlterTableOptions(
                operation, expectedIdentifier, Collections.singletonMap("connector", "dummy"));
        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table cat1.db1.tb1 reset ('connector')",
                                        SqlDialect.DEFAULT))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("ALTER TABLE RESET does not support changing 'connector'");

        assertThatThrownBy(() -> parse("alter table cat1.db1.tb1 reset ()", SqlDialect.DEFAULT))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("ALTER TABLE RESET does not support empty key");
    }

    @Test
    public void testAlterTableAddPkConstraint() throws Exception {
        prepareNonManagedTable(false);
        // Test alter add table constraint.
        Operation operation =
                parse(
                        "alter table tb1 add constraint ct1 primary key(a, b) not enforced",
                        SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(AlterTableAddConstraintOperation.class);
        AlterTableAddConstraintOperation addConstraintOperation =
                (AlterTableAddConstraintOperation) operation;
        assertThat(addConstraintOperation.asSummaryString())
                .isEqualTo(
                        "ALTER TABLE ADD CONSTRAINT: (identifier: [`cat1`.`db1`.`tb1`], "
                                + "constraintName: [ct1], columns: [a, b])");
        // Test alter table add pk on nullable column
        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb1 add constraint ct1 primary key(c) not enforced",
                                        SqlDialect.DEFAULT))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Could not create a PRIMARY KEY 'ct1'. Column 'c' is nullable.");
    }

    @Test
    public void testAlterTableAddPkConstraintEnforced() throws Exception {
        prepareNonManagedTable(false);
        // Test alter table add enforced
        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb1 add constraint ct1 primary key(a, b)",
                                        SqlDialect.DEFAULT))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. "
                                + "ENFORCED/NOT ENFORCED  controls if the constraint checks are performed on the "
                                + "incoming/outgoing data. Flink does not own the data therefore the "
                                + "only supported mode is the NOT ENFORCED mode");
    }

    @Test
    public void testAlterTableAddUniqueConstraint() throws Exception {
        prepareNonManagedTable(false);
        // Test alter add table constraint.
        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb1 add constraint ct1 unique(a, b) not enforced",
                                        SqlDialect.DEFAULT))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("UNIQUE constraint is not supported yet");
    }

    @Test
    public void testAlterTableAddUniqueConstraintEnforced() throws Exception {
        prepareNonManagedTable(false);
        // Test alter table add enforced
        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb1 add constraint ct1 unique(a, b)",
                                        SqlDialect.DEFAULT))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("UNIQUE constraint is not supported yet");
    }

    @Test
    public void testAlterTableDropConstraint() throws Exception {
        prepareNonManagedTable(true);
        // Test alter table add enforced
        Operation operation = parse("alter table tb1 drop constraint ct1", SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(AlterTableDropConstraintOperation.class);
        AlterTableDropConstraintOperation dropConstraint =
                (AlterTableDropConstraintOperation) operation;
        assertThat(dropConstraint.asSummaryString())
                .isEqualTo("ALTER TABLE cat1.db1.tb1 DROP CONSTRAINT ct1");
        assertThatThrownBy(() -> parse("alter table tb1 drop constraint ct2", SqlDialect.DEFAULT))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("CONSTRAINT [ct2] does not exist");
    }

    @Test
    public void testAlterTableCompactOnNonManagedTable() throws Exception {
        prepareNonManagedTable(false);
        assertThatThrownBy(() -> parse("alter table tb1 compact", SqlDialect.DEFAULT))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "ALTER TABLE COMPACT operation is not supported for non-managed table `cat1`.`db1`.`tb1`");
    }

    @Test
    public void testAlterTableCompactOnManagedNonPartitionedTable() throws Exception {
        prepareManagedTable(false);

        // specify partition on a non-partitioned table
        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb1 partition(dt = 'a') compact",
                                        SqlDialect.DEFAULT))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Partition column 'dt' not defined in the table schema. Table `cat1`.`db1`.`tb1` is not partitioned.");

        // alter a non-existed table
        assertThatThrownBy(() -> parse("alter table tb2 compact", SqlDialect.DEFAULT))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Table `cat1`.`db1`.`tb2` doesn't exist or is a temporary table.");

        checkAlterTableCompact(
                parse("alter table tb1 compact", SqlDialect.DEFAULT), Collections.emptyMap());
    }

    @Test
    public void testAlterTableCompactOnManagedPartitionedTable() throws Exception {
        prepareManagedTable(true);
        // compact partitioned table with a non-existed partition_spec
        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb1 partition (dt = 'a') compact",
                                        SqlDialect.DEFAULT))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Partition column 'dt' not defined in the table schema. Available ordered partition columns: ['b', 'c']");

        // compact partitioned table with full partition spec
        Map<String, String> staticPartitions = new HashMap<>();
        staticPartitions.put("b", "0");
        staticPartitions.put("c", "flink");
        checkAlterTableCompact(
                parse("alter table tb1 partition (b = 0, c = 'flink') compact", SqlDialect.DEFAULT),
                staticPartitions);

        // compact partitioned table with subordinate partition spec
        staticPartitions = Collections.singletonMap("b", "0");
        checkAlterTableCompact(
                parse("alter table tb1 partition (b = 0) compact", SqlDialect.DEFAULT),
                staticPartitions);

        // compact partitioned table with secondary partition spec
        staticPartitions = Collections.singletonMap("c", "flink");
        checkAlterTableCompact(
                parse("alter table tb1 partition (c = 'flink') compact", SqlDialect.DEFAULT),
                staticPartitions);

        // compact partitioned table without partition spec
        staticPartitions = Collections.emptyMap();
        checkAlterTableCompact(
                parse("alter table tb1 compact", SqlDialect.DEFAULT), staticPartitions);
    }

    @Test
    public void testCreateViewWithMatchRecognize() {
        Map<String, String> prop = new HashMap<>();
        prop.put("connector", "values");
        prop.put("bounded", "true");
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT().notNull())
                                .column("measurement", DataTypes.BIGINT().notNull())
                                .column(
                                        "ts",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("tmstmp", DataTypes.TIMESTAMP(3))))
                                .build(),
                        null,
                        Collections.emptyList(),
                        prop);

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "events"), false);

        final String sql =
                ""
                        + "CREATE TEMPORARY VIEW foo AS "
                        + "SELECT * "
                        + "FROM events MATCH_RECOGNIZE ("
                        + "    PARTITION BY id "
                        + "    ORDER BY ts ASC "
                        + "    MEASURES "
                        + "      next_step.measurement - this_step.measurement AS diff "
                        + "    AFTER MATCH SKIP TO NEXT ROW "
                        + "    PATTERN (this_step next_step)"
                        + "    DEFINE "
                        + "         this_step AS TRUE,"
                        + "         next_step AS TRUE"
                        + ")";

        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(CreateViewOperation.class);
    }

    @Test
    public void testCreateViewWithDynamicTableOptions() {
        Map<String, String> prop = new HashMap<>();
        prop.put("connector", "values");
        prop.put("bounded", "true");
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder()
                                .column("f0", DataTypes.INT())
                                .column("f1", DataTypes.VARCHAR(20))
                                .build(),
                        null,
                        Collections.emptyList(),
                        prop);

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "sourceA"), false);

        final String sql =
                ""
                        + "create view test_view as\n"
                        + "select *\n"
                        + "from sourceA /*+ OPTIONS('changelog-mode'='I') */";

        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(CreateViewOperation.class);
    }

    @Test
    public void testBeginStatementSet() {
        final String sql = "BEGIN STATEMENT SET";
        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(BeginStatementSetOperation.class);
        final BeginStatementSetOperation beginStatementSetOperation =
                (BeginStatementSetOperation) operation;

        assertThat(beginStatementSetOperation.asSummaryString()).isEqualTo("BEGIN STATEMENT SET");
    }

    @Test
    public void testEnd() {
        final String sql = "END";
        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(EndStatementSetOperation.class);
        final EndStatementSetOperation endStatementSetOperation =
                (EndStatementSetOperation) operation;

        assertThat(endStatementSetOperation.asSummaryString()).isEqualTo("END");
    }

    @Test
    public void testSqlRichExplainWithSelect() {
        final String sql = "explain plan for select a, b, c, d from t2";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(ExplainOperation.class);
    }

    @Test
    public void testSqlRichExplainWithInsert() {
        final String sql = "explain plan for insert into t1 select a, b, c, d from t2";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(ExplainOperation.class);
    }

    @Test
    public void testSqlRichExplainWithStatementSet() {
        final String sql =
                "explain plan for statement set begin "
                        + "insert into t1 select a, b, c, d from t2 where a > 1;"
                        + "insert into t1 select a, b, c, d from t2 where a > 2;"
                        + "end";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(ExplainOperation.class);
    }

    @Test
    public void testExplainDetailsWithSelect() {
        final String sql = "explain estimated_cost, changelog_mode select a, b, c, d from t2";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        assertExplainDetails(parse(sql, planner, parser));
    }

    @Test
    public void testExplainDetailsWithInsert() {
        final String sql =
                "explain estimated_cost, changelog_mode insert into t1 select a, b, c, d from t2";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        assertExplainDetails(parse(sql, planner, parser));
    }

    @Test
    public void testExplainDetailsWithStatementSet() {
        final String sql =
                "explain estimated_cost, changelog_mode statement set begin "
                        + "insert into t1 select a, b, c, d from t2 where a > 1;"
                        + "insert into t1 select a, b, c, d from t2 where a > 2;"
                        + "end";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        assertExplainDetails(parse(sql, planner, parser));
    }

    private void assertExplainDetails(Operation operation) {
        Set<String> expectedDetail = new HashSet<>();
        expectedDetail.add(ExplainDetail.ESTIMATED_COST.toString());
        expectedDetail.add(ExplainDetail.CHANGELOG_MODE.toString());
        assertThat(operation)
                .asInstanceOf(type(ExplainOperation.class))
                .satisfies(
                        explain ->
                                assertThat(explain.getExplainDetails()).isEqualTo(expectedDetail));
    }

    @Test
    public void testSqlExecuteWithStatementSet() {
        final String sql =
                "execute statement set begin "
                        + "insert into t1 select a, b, c, d from t2 where a > 1;"
                        + "insert into t1 select a, b, c, d from t2 where a > 2;"
                        + "end";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(StatementSetOperation.class);
    }

    @Test
    public void testSqlExecuteWithInsert() {
        final String sql = "execute insert into t1 select a, b, c, d from t2 where a > 1";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(SinkModifyOperation.class);
    }

    @Test
    public void testSqlExecuteWithSelect() {
        final String sql = "execute select a, b, c, d from t2 where a > 1";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(QueryOperation.class);
    }

    @Test
    public void testAddJar() {
        Arrays.asList(
                        "./test.\njar",
                        "file:///path/to/whatever",
                        "../test-jar.jar",
                        "/root/test.jar",
                        "test\\ jar.jar",
                        "oss://path/helloworld.go")
                .forEach(
                        jarPath -> {
                            AddJarOperation operation =
                                    (AddJarOperation)
                                            parser.parse(String.format("ADD JAR '%s'", jarPath))
                                                    .get(0);
                            assertThat(operation.getPath()).isEqualTo(jarPath);
                        });
    }

    @Test
    public void testRemoveJar() {
        Arrays.asList(
                        "./test.\njar",
                        "file:///path/to/whatever",
                        "../test-jar.jar",
                        "/root/test.jar",
                        "test\\ jar.jar",
                        "oss://path/helloworld.go")
                .forEach(
                        jarPath -> {
                            RemoveJarOperation operation =
                                    (RemoveJarOperation)
                                            parser.parse(String.format("REMOVE JAR '%s'", jarPath))
                                                    .get(0);
                            assertThat(operation.getPath()).isEqualTo(jarPath);
                        });
    }

    @Test
    public void testShowJars() {
        final String sql = "SHOW JARS";
        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(ShowJarsOperation.class);
        final ShowJarsOperation showModulesOperation = (ShowJarsOperation) operation;
        assertThat(showModulesOperation.asSummaryString()).isEqualTo("SHOW JARS");
    }

    @Test
    public void testSet() {
        Operation operation1 = parse("SET", SqlDialect.DEFAULT);
        assertThat(operation1).isInstanceOf(SetOperation.class);
        SetOperation setOperation1 = (SetOperation) operation1;
        assertThat(setOperation1.getKey()).isNotPresent();
        assertThat(setOperation1.getValue()).isNotPresent();

        Operation operation2 = parse("SET 'test-key' = 'test-value'", SqlDialect.DEFAULT);
        assertThat(operation2).isInstanceOf(SetOperation.class);
        SetOperation setOperation2 = (SetOperation) operation2;
        assertThat(setOperation2.getKey()).hasValue("test-key");
        assertThat(setOperation2.getValue()).hasValue("test-value");
    }

    @Test
    public void testReset() {
        Operation operation1 = parse("RESET", SqlDialect.DEFAULT);
        assertThat(operation1).isInstanceOf(ResetOperation.class);
        assertThat(((ResetOperation) operation1).getKey()).isNotPresent();

        Operation operation2 = parse("RESET 'test-key'", SqlDialect.DEFAULT);
        assertThat(operation2).isInstanceOf(ResetOperation.class);
        assertThat(((ResetOperation) operation2).getKey()).isPresent();
        assertThat(((ResetOperation) operation2).getKey()).hasValue("test-key");
    }

    @ParameterizedTest
    @ValueSource(strings = {"SET", "SET;", "SET ;", "SET\t;", "SET\n;"})
    public void testSetCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(SetOperation.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"HELP", "HELP;", "HELP ;", "HELP\t;", "HELP\n;"})
    public void testHelpCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(HelpOperation.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"CLEAR", "CLEAR;", "CLEAR ;", "CLEAR\t;", "CLEAR\n;"})
    public void testClearCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(ClearOperation.class);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "QUIT;", "QUIT;", "QUIT ;", "QUIT\t;", "QUIT\n;", "EXIT;", "EXIT ;", "EXIT\t;",
                "EXIT\n;", "EXIT ; "
            })
    public void testQuitCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(QuitOperation.class);
    }

    // ~ Tool Methods ----------------------------------------------------------

    private static TestItem createTestItem(Object... args) {
        assert args.length == 2;
        final String testExpr = (String) args[0];
        TestItem testItem = TestItem.fromTestExpr(testExpr);
        if (args[1] instanceof String) {
            testItem.withExpectedError((String) args[1]);
        } else {
            testItem.withExpectedType(args[1]);
        }
        return testItem;
    }

    private void checkExplainSql(String sql) {
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        SqlNode node = parser.parse(sql);
        assertThat(node).isInstanceOf(SqlRichExplain.class);
        Operation operation = SqlToOperationConverter.convert(planner, catalogManager, node).get();
        assertThat(operation).isInstanceOf(ExplainOperation.class);
    }

    private void assertShowFunctions(
            String sql, String expectedSummary, FunctionScope expectedScope) {
        Operation operation = parse(sql, SqlDialect.DEFAULT);
        assertThat(operation).isInstanceOf(ShowFunctionsOperation.class);

        final ShowFunctionsOperation showFunctionsOperation = (ShowFunctionsOperation) operation;

        assertThat(showFunctionsOperation.getFunctionScope()).isEqualTo(expectedScope);
        assertThat(showFunctionsOperation.asSummaryString()).isEqualTo(expectedSummary);
    }

    private void assertAlterTableOptions(
            Operation operation,
            ObjectIdentifier expectedIdentifier,
            Map<String, String> expectedOptions) {
        assertThat(operation).isInstanceOf(AlterTableOptionsOperation.class);
        final AlterTableOptionsOperation alterTableOptionsOperation =
                (AlterTableOptionsOperation) operation;
        assertThat(alterTableOptionsOperation.getTableIdentifier()).isEqualTo(expectedIdentifier);
        assertThat(alterTableOptionsOperation.getCatalogTable().getOptions())
                .isEqualTo(expectedOptions);
    }

    private Operation parse(String sql, FlinkPlannerImpl planner, CalciteParser parser) {
        SqlNode node = parser.parse(sql);
        return SqlToOperationConverter.convert(planner, catalogManager, node).get();
    }

    private Operation parse(String sql, SqlDialect sqlDialect) {
        FlinkPlannerImpl planner = getPlannerBySqlDialect(sqlDialect);
        final CalciteParser parser = getParserBySqlDialect(sqlDialect);
        SqlNode node = parser.parse(sql);
        return SqlToOperationConverter.convert(planner, catalogManager, node).get();
    }

    private void prepareNonManagedTable(boolean hasConstraint) throws Exception {
        prepareTable(false, false, hasConstraint);
    }

    private void prepareManagedTable(boolean hasPartition) throws Exception {
        TestManagedTableFactory.MANAGED_TABLES.put(
                ObjectIdentifier.of("cat1", "db1", "tb1"), new AtomicReference<>());
        prepareTable(true, hasPartition, false);
    }

    private void prepareTable(boolean managedTable, boolean hasPartition, boolean hasConstraint)
            throws Exception {
        Catalog catalog = new GenericInMemoryCatalog("default", "default");
        catalogManager.registerCatalog("cat1", catalog);
        catalog.createDatabase("db1", new CatalogDatabaseImpl(new HashMap<>(), null), true);
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("a", DataTypes.STRING().notNull())
                        .column("b", DataTypes.BIGINT().notNull())
                        .column("c", DataTypes.BIGINT());
        Map<String, String> options = new HashMap<>();
        options.put("k", "v");
        if (!managedTable) {
            options.put("connector", "dummy");
        }
        CatalogTable catalogTable =
                CatalogTable.of(
                        hasConstraint
                                ? builder.primaryKeyNamed("ct1", "a", "b").build()
                                : builder.build(),
                        "tb1",
                        hasPartition ? Arrays.asList("b", "c") : Collections.emptyList(),
                        Collections.unmodifiableMap(options));
        catalogManager.setCurrentCatalog("cat1");
        catalogManager.setCurrentDatabase("db1");
        ObjectIdentifier tableIdentifier = ObjectIdentifier.of("cat1", "db1", "tb1");
        catalogManager.createTable(catalogTable, tableIdentifier, true);
    }

    private FlinkPlannerImpl getPlannerBySqlDialect(SqlDialect sqlDialect) {
        tableConfig.setSqlDialect(sqlDialect);
        return plannerContext.createFlinkPlanner(
                catalogManager.getCurrentCatalog(), catalogManager.getCurrentDatabase());
    }

    private CalciteParser getParserBySqlDialect(SqlDialect sqlDialect) {
        tableConfig.setSqlDialect(sqlDialect);
        return plannerContext.createCalciteParser();
    }

    private void checkAlterTableCompact(Operation operation, Map<String, String> staticPartitions) {
        assertThat(operation).isInstanceOf(SinkModifyOperation.class);
        SinkModifyOperation modifyOperation = (SinkModifyOperation) operation;
        assertThat(modifyOperation.getStaticPartitions())
                .containsExactlyInAnyOrderEntriesOf(staticPartitions);
        assertThat(modifyOperation.isOverwrite()).isFalse();
        assertThat(modifyOperation.getDynamicOptions())
                .containsEntry(
                        TestManagedTableFactory.ENRICHED_KEY,
                        TestManagedTableFactory.ENRICHED_VALUE);
        ContextResolvedTable contextResolvedTable = modifyOperation.getContextResolvedTable();
        assertThat(contextResolvedTable.getIdentifier())
                .isEqualTo(ObjectIdentifier.of("cat1", "db1", "tb1"));
        assertThat(modifyOperation.getChild()).isInstanceOf(SourceQueryOperation.class);
        SourceQueryOperation child = (SourceQueryOperation) modifyOperation.getChild();
        assertThat(child.getChildren()).isEmpty();
        assertThat(child.getDynamicOptions()).containsEntry("k", "v");
        assertThat(child.getDynamicOptions())
                .containsEntry(
                        TestManagedTableFactory.ENRICHED_KEY,
                        TestManagedTableFactory.ENRICHED_VALUE);
    }

    // ~ Inner Classes ----------------------------------------------------------

    private static class TestItem {
        private final String testExpr;
        @Nullable private Object expectedType;
        @Nullable private String expectedError;

        private TestItem(String testExpr) {
            this.testExpr = testExpr;
        }

        static TestItem fromTestExpr(String testExpr) {
            return new TestItem(testExpr);
        }

        TestItem withExpectedType(Object expectedType) {
            this.expectedType = expectedType;
            return this;
        }

        TestItem withExpectedError(String expectedError) {
            this.expectedError = expectedError;
            return this;
        }

        @Override
        public String toString() {
            return this.testExpr;
        }
    }

    private Operation parseAndConvert(String sql) {
        final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);

        SqlNode node = parser.parse(sql);
        return SqlToOperationConverter.convert(planner, catalogManager, node).get();
    }
}
