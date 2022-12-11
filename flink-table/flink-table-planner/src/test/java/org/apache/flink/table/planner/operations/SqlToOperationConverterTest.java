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
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.factories.TestManagedTableFactory;
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
import org.apache.flink.table.operations.ddl.AlterTableDropConstraintOperation;
import org.apache.flink.table.operations.ddl.AlterTableOptionsOperation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;
import org.apache.flink.table.operations.ddl.AlterTableSchemaOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateDatabaseOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateTempSystemFunctionOperation;
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
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;
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
    private final TableConfig tableConfig = TableConfig.getDefault();
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

    private final PlannerMocks plannerMocks =
            PlannerMocks.newBuilder()
                    .withBatchMode(true)
                    .withTableConfig(tableConfig)
                    .withCatalogManager(catalogManager)
                    .withRootSchema(
                            asRootSchema(
                                    new CatalogManagerCalciteSchema(
                                            catalogManager, isStreamingMode)))
                    .build();
    private final PlannerContext plannerContext = plannerMocks.getPlannerContext();
    private final FunctionCatalog functionCatalog = plannerMocks.getFunctionCatalog();

    private final Supplier<FlinkPlannerImpl> plannerSupplier = plannerContext::createFlinkPlanner;

    private final Parser parser =
            new ParserImpl(
                    catalogManager,
                    plannerSupplier,
                    () -> plannerSupplier.get().parser(),
                    plannerContext.getRexFactory());

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
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(UseCatalogOperation.class);
        assertThat(((UseCatalogOperation) operation).getCatalogName()).isEqualTo("cat1");
        assertThat(operation.asSummaryString()).isEqualTo("USE CATALOG cat1");
    }

    @Test
    public void testUseDatabase() {
        final String sql1 = "USE db1";
        Operation operation1 = parse(sql1);
        assertThat(operation1).isInstanceOf(UseDatabaseOperation.class);
        assertThat(((UseDatabaseOperation) operation1).getCatalogName()).isEqualTo("builtin");
        assertThat(((UseDatabaseOperation) operation1).getDatabaseName()).isEqualTo("db1");

        final String sql2 = "USE cat1.db1";
        Operation operation2 = parse(sql2);
        assertThat(operation2).isInstanceOf(UseDatabaseOperation.class);
        assertThat(((UseDatabaseOperation) operation2).getCatalogName()).isEqualTo("cat1");
        assertThat(((UseDatabaseOperation) operation2).getDatabaseName()).isEqualTo("db1");
    }

    @Test
    public void testUseDatabaseWithException() {
        final String sql = "USE cat1.db1.tbl1";
        assertThatThrownBy(() -> parse(sql)).isInstanceOf(ValidationException.class);
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
            Operation operation = parse(createDatabaseSqls[i]);
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
            Operation operation = parse(dropDatabaseSqls[i]);
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
        Operation operation = parse(sql);
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

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(LoadModuleOperation.class);
        final LoadModuleOperation loadModuleOperation = (LoadModuleOperation) operation;

        assertThat(loadModuleOperation.getModuleName()).isEqualTo(expectedModuleName);
        assertThat(loadModuleOperation.getOptions()).isEqualTo(expectedOptions);
    }

    @Test
    public void testUnloadModule() {
        final String sql = "UNLOAD MODULE dummy";
        final String expectedModuleName = "dummy";

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(UnloadModuleOperation.class);

        final UnloadModuleOperation unloadModuleOperation = (UnloadModuleOperation) operation;

        assertThat(unloadModuleOperation.getModuleName()).isEqualTo(expectedModuleName);
    }

    @Test
    public void testUseOneModule() {
        final String sql = "USE MODULES dummy";
        final List<String> expectedModuleNames = Collections.singletonList("dummy");

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(UseModulesOperation.class);

        final UseModulesOperation useModulesOperation = (UseModulesOperation) operation;

        assertThat(useModulesOperation.getModuleNames()).isEqualTo(expectedModuleNames);
        assertThat(useModulesOperation.asSummaryString()).isEqualTo("USE MODULES: [dummy]");
    }

    @Test
    public void testUseMultipleModules() {
        final String sql = "USE MODULES x, y, z";
        final List<String> expectedModuleNames = Arrays.asList("x", "y", "z");

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(UseModulesOperation.class);

        final UseModulesOperation useModulesOperation = (UseModulesOperation) operation;

        assertThat(useModulesOperation.getModuleNames()).isEqualTo(expectedModuleNames);
        assertThat(useModulesOperation.asSummaryString()).isEqualTo("USE MODULES: [x, y, z]");
    }

    @Test
    public void testShowModules() {
        final String sql = "SHOW MODULES";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowModulesOperation.class);
        final ShowModulesOperation showModulesOperation = (ShowModulesOperation) operation;

        assertThat(showModulesOperation.requireFull()).isFalse();
        assertThat(showModulesOperation.asSummaryString()).isEqualTo("SHOW MODULES");
    }

    @Test
    public void testShowTables() {
        final String sql = "SHOW TABLES from cat1.db1 not like 't%'";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowTablesOperation.class);

        ShowTablesOperation showTablesOperation = (ShowTablesOperation) operation;
        assertThat(showTablesOperation.getCatalogName()).isEqualTo("cat1");
        assertThat(showTablesOperation.getDatabaseName()).isEqualTo("db1");
        assertThat(showTablesOperation.getPreposition()).isEqualTo("FROM");
        assertThat(showTablesOperation.isUseLike()).isTrue();
        assertThat(showTablesOperation.isNotLike()).isTrue();

        final String sql2 = "SHOW TABLES in db2";
        showTablesOperation = (ShowTablesOperation) parse(sql2);
        assertThat(showTablesOperation.getCatalogName()).isEqualTo("builtin");
        assertThat(showTablesOperation.getDatabaseName()).isEqualTo("db2");
        assertThat(showTablesOperation.getPreposition()).isEqualTo("IN");
        assertThat(showTablesOperation.isUseLike()).isFalse();
        assertThat(showTablesOperation.isNotLike()).isFalse();

        final String sql3 = "SHOW TABLES";
        showTablesOperation = (ShowTablesOperation) parse(sql3);
        assertThat(showTablesOperation.getCatalogName()).isNull();
        assertThat(showTablesOperation.getDatabaseName()).isNull();
        assertThat(showTablesOperation.getPreposition()).isNull();
    }

    @Test
    public void testShowFullModules() {
        final String sql = "SHOW FULL MODULES";
        Operation operation = parse(sql);
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
    public void testCreateFunction() {
        // test create catalog function
        String sql =
                "CREATE FUNCTION test_udf AS 'org.apache.fink.function.function1' "
                        + "LANGUAGE JAVA USING JAR 'file:///path/to/test.jar'";
        final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, getParserBySqlDialect(SqlDialect.DEFAULT));
        assertThat(operation).isInstanceOf(CreateCatalogFunctionOperation.class);
        CatalogFunction actualFunction =
                ((CreateCatalogFunctionOperation) operation).getCatalogFunction();

        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "CREATE CATALOG FUNCTION: (catalogFunction: [Optional[This is a user-defined function]], "
                                + "identifier: [`builtin`.`default`.`test_udf`], ignoreIfExists: [false], isTemporary: [false])");

        CatalogFunction expected =
                new CatalogFunctionImpl(
                        "org.apache.fink.function.function1",
                        FunctionLanguage.JAVA,
                        Collections.singletonList(
                                new ResourceUri(ResourceType.JAR, "file:///path/to/test.jar")));
        assertThat(actualFunction).isEqualTo(expected);

        // test create temporary system function
        sql =
                "CREATE TEMPORARY SYSTEM FUNCTION test_udf2 AS 'org.apache.fink.function.function2' "
                        + "LANGUAGE SCALA USING JAR 'file:///path/to/test.jar'";
        operation = parse(sql, planner, getParserBySqlDialect(SqlDialect.DEFAULT));

        assertThat(operation).isInstanceOf(CreateTempSystemFunctionOperation.class);
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "CREATE TEMPORARY SYSTEM FUNCTION: (functionName: [test_udf2], "
                                + "catalogFunction: [CatalogFunctionImpl{className='org.apache.fink.function.function2', "
                                + "functionLanguage='SCALA', "
                                + "functionResource='[ResourceUri{resourceType=JAR, uri='file:///path/to/test.jar'}]'}], "
                                + "ignoreIfExists: [false], functionLanguage: [SCALA])");
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
            Operation operation = parse(renameTableSqls[i]);
            assertThat(operation).isInstanceOf(AlterTableRenameOperation.class);
            final AlterTableRenameOperation alterTableRenameOperation =
                    (AlterTableRenameOperation) operation;
            assertThat(alterTableRenameOperation.getTableIdentifier())
                    .isEqualTo(expectedIdentifier);
            assertThat(alterTableRenameOperation.getNewTableIdentifier())
                    .isEqualTo(expectedNewIdentifier);
        }
        // test alter table options
        Operation operation = parse("alter table cat1.db1.tb1 set ('k1' = 'v1', 'K2' = 'V2')");
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("connector", "dummy");
        expectedOptions.put("k", "v");
        expectedOptions.put("k1", "v1");
        expectedOptions.put("K2", "V2");

        assertAlterTableOptions(operation, expectedIdentifier, expectedOptions);

        // test alter table reset
        operation = parse("alter table cat1.db1.tb1 reset ('k')");
        assertAlterTableOptions(
                operation, expectedIdentifier, Collections.singletonMap("connector", "dummy"));
        assertThatThrownBy(() -> parse("alter table cat1.db1.tb1 reset ('connector')"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("ALTER TABLE RESET does not support changing 'connector'");

        assertThatThrownBy(() -> parse("alter table cat1.db1.tb1 reset ()"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("ALTER TABLE RESET does not support empty key");
    }

    @Test
    public void testAlterTableDropConstraint() throws Exception {
        prepareNonManagedTable(true);
        // Test alter table add enforced
        Operation operation = parse("alter table tb1 drop constraint ct1");
        assertThat(operation).isInstanceOf(AlterTableDropConstraintOperation.class);
        AlterTableDropConstraintOperation dropConstraint =
                (AlterTableDropConstraintOperation) operation;
        assertThat(dropConstraint.asSummaryString())
                .isEqualTo("ALTER TABLE cat1.db1.tb1 DROP CONSTRAINT ct1");
        assertThatThrownBy(() -> parse("alter table tb1 drop constraint ct2"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("CONSTRAINT [ct2] does not exist");
    }

    @Test
    public void testAlterTableCompactOnNonManagedTable() throws Exception {
        prepareNonManagedTable(false);
        assertThatThrownBy(() -> parse("alter table tb1 compact"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "ALTER TABLE COMPACT operation is not supported for non-managed table `cat1`.`db1`.`tb1`");
    }

    @Test
    public void testAlterTableCompactOnManagedNonPartitionedTable() throws Exception {
        prepareManagedTable(false);

        // specify partition on a non-partitioned table
        assertThatThrownBy(() -> parse("alter table tb1 partition(dt = 'a') compact"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Partition column 'dt' not defined in the table schema. Table `cat1`.`db1`.`tb1` is not partitioned.");

        // alter a non-existed table
        assertThatThrownBy(() -> parse("alter table tb2 compact"))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Table `cat1`.`db1`.`tb2` doesn't exist or is a temporary table.");

        checkAlterTableCompact(parse("alter table tb1 compact"), Collections.emptyMap());
    }

    @Test
    public void testAlterTableCompactOnManagedPartitionedTable() throws Exception {
        prepareManagedTable(true);
        // compact partitioned table with a non-existed partition_spec
        assertThatThrownBy(() -> parse("alter table tb1 partition (dt = 'a') compact"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Partition column 'dt' not defined in the table schema. Available ordered partition columns: ['b', 'c']");

        // compact partitioned table with full partition spec
        Map<String, String> staticPartitions = new HashMap<>();
        staticPartitions.put("b", "0");
        staticPartitions.put("c", "flink");
        checkAlterTableCompact(
                parse("alter table tb1 partition (b = 0, c = 'flink') compact"), staticPartitions);

        // compact partitioned table with subordinate partition spec
        staticPartitions = Collections.singletonMap("b", "0");
        checkAlterTableCompact(
                parse("alter table tb1 partition (b = 0) compact"), staticPartitions);

        // compact partitioned table with secondary partition spec
        staticPartitions = Collections.singletonMap("c", "flink");
        checkAlterTableCompact(
                parse("alter table tb1 partition (c = 'flink') compact"), staticPartitions);

        // compact partitioned table without partition spec
        staticPartitions = Collections.emptyMap();
        checkAlterTableCompact(parse("alter table tb1 compact"), staticPartitions);
    }

    @Test
    public void testFailedToAlterTableAddColumn() throws Exception {
        prepareNonManagedTable("tb1", 0);

        // try to add a column with duplicated name
        assertThatThrownBy(() -> parse("alter table tb1 add a bigint"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Try to add a column `a` which already exists in the table.");

        // try to add multiple columns with duplicated column name
        assertThatThrownBy(() -> parse("alter table tb1 add (x array<string>, x string)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Encounter duplicate column `x`.");

        // refer to a nonexistent column
        assertThatThrownBy(() -> parse("alter table tb1 add x bigint after y"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Referenced column `y` by 'AFTER' does not exist in the table.");

        // refer to a new added column that appears in the post position
        assertThatThrownBy(() -> parse("alter table tb1 add (x bigint after y, y string first)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Referenced column `y` by 'AFTER' does not exist in the table.");

        // add a computed column based on nonexistent column
        assertThatThrownBy(() -> parse("alter table tb1 add m as n + 2"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Invalid expression for computed column 'm'.");

        // add a computed column based on another computed column
        assertThatThrownBy(() -> parse("alter table tb1 add (m as b * 2, n as m + 2)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Invalid expression for computed column 'n'.");
        // invalid expression
        assertThatThrownBy(() -> parse("alter table tb1 add (m as 'hello' || b)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Invalid expression for computed column 'm'.");

        // add an inner field to a nested row
        assertThatThrownBy(() -> parse("alter table tb1 add (e.f3 string)"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Alter nested row type is not supported yet.");

        // refer to a nested inner field
        assertThatThrownBy(() -> parse("alter table tb1 add (x string after e.f2)"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Alter nested row type is not supported yet.");

        assertThatThrownBy(() -> parse("alter table tb1 add (e.f3 string after e.f1)"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Alter nested row type is not supported yet.");
    }

    @Test
    public void testAlterTableAddColumn() throws Exception {
        prepareNonManagedTable("tb1", 0);

        ObjectIdentifier tableIdentifier = ObjectIdentifier.of("cat1", "db1", "tb1");
        Schema originalSchema =
                catalogManager.getTable(tableIdentifier).get().getTable().getUnresolvedSchema();

        // add a single column
        Operation operation =
                parse("alter table tb1 add h double not null comment 'h is double not null'");
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "ALTER TABLE cat1.db1.tb1 SET SCHEMA (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT NOT NULL,\n"
                                + "  `c` STRING,\n"
                                + "  `d` AS [a*(b+2 + a*b)],\n"
                                + "  `e` ROW<`f0` STRING, `f1` INT, `f2` ROW<`f0` DOUBLE, `f1` ARRAY<FLOAT>>>,\n"
                                + "  `f` AS [e.f1 + e.f2.f0],\n"
                                + "  `g` METADATA VIRTUAL,\n"
                                + "  `ts` TIMESTAMP(3) COMMENT 'just a comment',\n"
                                + "  `h` DOUBLE NOT NULL COMMENT 'h is double not null'\n"
                                + ")");
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .fromSchema(originalSchema)
                        .column("h", DataTypes.DOUBLE().notNull())
                        .withComment("h is double not null")
                        .build());

        // add multiple columns with pk, computed/metadata column
        operation =
                parse(
                        "alter table tb1 add (\n"
                                + " h as e.f2.f1 first,\n"
                                + " i as b*2 after b,\n"
                                + " j int metadata from 'mk1' virtual comment 'comment_metadata' first,\n"
                                + " k string primary key not enforced after h)");
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "ALTER TABLE cat1.db1.tb1 SET SCHEMA (\n"
                                + "  `j` METADATA FROM 'mk1' VIRTUAL COMMENT 'comment_metadata',\n"
                                + "  `h` AS [`e`.`f2`.`f1`],\n"
                                + "  `k` STRING NOT NULL,\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT NOT NULL,\n"
                                + "  `i` AS [`b` * 2],\n"
                                + "  `c` STRING,\n"
                                + "  `d` AS [a*(b+2 + a*b)],\n"
                                + "  `e` ROW<`f0` STRING, `f1` INT, `f2` ROW<`f0` DOUBLE, `f1` ARRAY<FLOAT>>>,\n"
                                + "  `f` AS [e.f1 + e.f2.f0],\n"
                                + "  `g` METADATA VIRTUAL,\n"
                                + "  `ts` TIMESTAMP(3) COMMENT 'just a comment',\n"
                                + "  CONSTRAINT `PK_k` PRIMARY KEY (`k`) NOT ENFORCED\n"
                                + ")");
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .columnByMetadata("j", DataTypes.INT(), "mk1", true)
                        .withComment("comment_metadata")
                        .columnByExpression("h", "`e`.`f2`.`f1`")
                        .column("k", DataTypes.STRING().notNull())
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.BIGINT().notNull())
                        .columnByExpression("i", new SqlCallExpression("`b` * 2"))
                        .column("c", DataTypes.STRING())
                        .columnByExpression("d", "a*(b+2 + a*b)")
                        .column(
                                "e",
                                DataTypes.ROW(
                                        DataTypes.STRING(),
                                        DataTypes.INT(),
                                        DataTypes.ROW(
                                                DataTypes.DOUBLE(),
                                                DataTypes.ARRAY(DataTypes.FLOAT()))))
                        .columnByExpression("f", "e.f1 + e.f2.f0")
                        .columnByMetadata("g", DataTypes.STRING(), null, true)
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .withComment("just a comment")
                        .primaryKey("k")
                        .build());

        // add nested type
        operation =
                parse(
                        "alter table tb1 add (\n"
                                + " r row<r1 bigint, r2 string, r3 array<double> not null> not null comment 'add composite type',\n"
                                + " m map<string not null, int not null>,\n"
                                + " n as r.r1 * 2 after r,\n"
                                + " tss as to_timestamp(r.r2) comment 'rowtime' after ts,\n"
                                + " na as r.r3 after ts)");
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "ALTER TABLE cat1.db1.tb1 SET SCHEMA (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT NOT NULL,\n"
                                + "  `c` STRING,\n"
                                + "  `d` AS [a*(b+2 + a*b)],\n"
                                + "  `e` ROW<`f0` STRING, `f1` INT, `f2` ROW<`f0` DOUBLE, `f1` ARRAY<FLOAT>>>,\n"
                                + "  `f` AS [e.f1 + e.f2.f0],\n"
                                + "  `g` METADATA VIRTUAL,\n"
                                + "  `ts` TIMESTAMP(3) COMMENT 'just a comment',\n"
                                + "  `na` AS [`r`.`r3`],\n"
                                + "  `tss` AS [`to_timestamp`(`r`.`r2`)] COMMENT 'rowtime',\n"
                                + "  `r` ROW<`r1` BIGINT, `r2` STRING, `r3` ARRAY<DOUBLE> NOT NULL> NOT NULL COMMENT 'add composite type',\n"
                                + "  `n` AS [`r`.`r1` * 2],\n"
                                + "  `m` MAP<STRING NOT NULL, INT NOT NULL>\n"
                                + ")");
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .fromSchema(originalSchema)
                        .columnByExpression("na", "`r`.`r3`")
                        .columnByExpression("tss", "`to_timestamp`(`r`.`r2`)")
                        .withComment("rowtime")
                        .column(
                                "r",
                                DataTypes.ROW(
                                                DataTypes.FIELD("r1", DataTypes.BIGINT()),
                                                DataTypes.FIELD("r2", DataTypes.STRING()),
                                                DataTypes.FIELD(
                                                        "r3",
                                                        DataTypes.ARRAY(DataTypes.DOUBLE())
                                                                .notNull()))
                                        .notNull())
                        .withComment("add composite type")
                        .columnByExpression("n", "`r`.`r1` * 2")
                        .column(
                                "m",
                                DataTypes.MAP(
                                        DataTypes.STRING().notNull(), DataTypes.INT().notNull()))
                        .build());
    }

    @Test
    public void testFailedToAlterTableAddPk() throws Exception {
        // the original table has one pk
        prepareNonManagedTable("tb1", 1);

        assertThatThrownBy(() -> parse("alter table tb1 add primary key(c) not enforced"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "The base table has already defined the primary key constraint [`a`]. "
                                + "You might want to drop it before adding a new one.");

        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb1 add x string not null primary key not enforced"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "The base table has already defined the primary key constraint [`a`]. "
                                + "You might want to drop it before adding a new one");

        // the original table has composite pk
        prepareNonManagedTable("tb2", 2);

        assertThatThrownBy(() -> parse("alter table tb2 add primary key(c) not enforced"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "The base table has already defined the primary key constraint [`a`, `b`]. "
                                + "You might want to drop it before adding a new one");

        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb2 add x string not null primary key not enforced"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "The base table has already defined the primary key constraint [`a`, `b`]. "
                                + "You might want to drop it before adding a new one");

        // the original table does not define pk
        prepareNonManagedTable("tb3", 0);

        // specify a nonexistent column as pk
        assertThatThrownBy(() -> parse("alter table tb3 add primary key (x) not enforced"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\nInvalid primary key 'PK_x'. Column 'x' does not exist.");

        // add unique constraint
        assertThatThrownBy(() -> parse("alter table tb3 add unique(b)"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("UNIQUE constraint is not supported yet");

        // lack NOT ENFORCED
        assertThatThrownBy(() -> parse("alter table tb3 add primary key(b)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint");

        // add a composite pk which contains computed column
        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb3 add (\n"
                                                + "  x as upper(c),\n"
                                                + "  primary key (d, x) not enforced)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Invalid primary key 'PK_d_x'. Column 'd' is not a physical column.");

        // add a pk which is metadata column
        assertThatThrownBy(() -> parse("alter table tb3 add (primary key (g) not enforced)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Invalid primary key 'PK_g'. Column 'g' is not a physical column.");
    }

    @Test
    public void testAlterTableAddPrimaryKey() throws Exception {
        prepareNonManagedTable("tb1", 0);

        ObjectIdentifier tableIdentifier = ObjectIdentifier.of("cat1", "db1", "tb1");
        Schema originalSchema =
                catalogManager.getTable(tableIdentifier).get().getTable().getUnresolvedSchema();
        Operation operation =
                parse("alter table tb1 add constraint my_pk primary key (a, b) not enforced");
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "ALTER TABLE cat1.db1.tb1 SET SCHEMA (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT NOT NULL,\n"
                                + "  `c` STRING,\n"
                                + "  `d` AS [a*(b+2 + a*b)],\n"
                                + "  `e` ROW<`f0` STRING, `f1` INT, `f2` ROW<`f0` DOUBLE, `f1` ARRAY<FLOAT>>>,\n"
                                + "  `f` AS [e.f1 + e.f2.f0],\n"
                                + "  `g` METADATA VIRTUAL,\n"
                                + "  `ts` TIMESTAMP(3) COMMENT 'just a comment',\n"
                                + "  CONSTRAINT `my_pk` PRIMARY KEY (`a`, `b`) NOT ENFORCED\n"
                                + ")");
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .fromSchema(originalSchema)
                        .primaryKeyNamed("my_pk", "a", "b")
                        .build());

        operation = parse("alter table tb1 add x bigint not null primary key not enforced");
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "ALTER TABLE cat1.db1.tb1 SET SCHEMA (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT NOT NULL,\n"
                                + "  `c` STRING,\n"
                                + "  `d` AS [a*(b+2 + a*b)],\n"
                                + "  `e` ROW<`f0` STRING, `f1` INT, `f2` ROW<`f0` DOUBLE, `f1` ARRAY<FLOAT>>>,\n"
                                + "  `f` AS [e.f1 + e.f2.f0],\n"
                                + "  `g` METADATA VIRTUAL,\n"
                                + "  `ts` TIMESTAMP(3) COMMENT 'just a comment',\n"
                                + "  `x` BIGINT NOT NULL,\n"
                                + "  CONSTRAINT `PK_x` PRIMARY KEY (`x`) NOT ENFORCED\n"
                                + ")");
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .fromSchema(originalSchema)
                        .column("x", DataTypes.BIGINT().notNull())
                        .primaryKey("x")
                        .build());

        // implicit nullability conversion
        operation = parse("alter table tb1 add x bigint primary key not enforced");
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "ALTER TABLE cat1.db1.tb1 SET SCHEMA (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT NOT NULL,\n"
                                + "  `c` STRING,\n"
                                + "  `d` AS [a*(b+2 + a*b)],\n"
                                + "  `e` ROW<`f0` STRING, `f1` INT, `f2` ROW<`f0` DOUBLE, `f1` ARRAY<FLOAT>>>,\n"
                                + "  `f` AS [e.f1 + e.f2.f0],\n"
                                + "  `g` METADATA VIRTUAL,\n"
                                + "  `ts` TIMESTAMP(3) COMMENT 'just a comment',\n"
                                + "  `x` BIGINT NOT NULL,\n"
                                + "  CONSTRAINT `PK_x` PRIMARY KEY (`x`) NOT ENFORCED\n"
                                + ")");
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .fromSchema(originalSchema)
                        .column("x", DataTypes.BIGINT().notNull())
                        .primaryKey("x")
                        .build());

        operation = parse("alter table tb1 add constraint ct primary key(ts) not enforced");
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "ALTER TABLE cat1.db1.tb1 SET SCHEMA (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT NOT NULL,\n"
                                + "  `c` STRING,\n"
                                + "  `d` AS [a*(b+2 + a*b)],\n"
                                + "  `e` ROW<`f0` STRING, `f1` INT, `f2` ROW<`f0` DOUBLE, `f1` ARRAY<FLOAT>>>,\n"
                                + "  `f` AS [e.f1 + e.f2.f0],\n"
                                + "  `g` METADATA VIRTUAL,\n"
                                + "  `ts` TIMESTAMP(3) NOT NULL COMMENT 'just a comment',\n"
                                + "  CONSTRAINT `ct` PRIMARY KEY (`ts`) NOT ENFORCED\n"
                                + ")");
        List<Schema.UnresolvedColumn> subColumns =
                originalSchema.getColumns().subList(0, originalSchema.getColumns().size() - 1);
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .fromColumns(subColumns)
                        .column("ts", DataTypes.TIMESTAMP(3).notNull())
                        .withComment("just a comment")
                        .primaryKeyNamed("ct", "ts")
                        .build());
    }

    @Test
    public void testFailedToAlterTableAddWatermark() throws Exception {
        prepareNonManagedTable("tb1", false);

        // add watermark with an undefined column as rowtime
        assertThatThrownBy(() -> parse("alter table tb1 add watermark for x as x"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Invalid column name 'x' for rowtime attribute in watermark declaration. "
                                + "Available columns are: [a, b, c, d, e, f, g, ts]");

        // add watermark with invalid type
        assertThatThrownBy(() -> parse("alter table tb1 add watermark for b as b"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Invalid data type of time field for watermark definition. "
                                + "The field must be of type TIMESTAMP(p) or TIMESTAMP_LTZ(p), "
                                + "the supported precision 'p' is from 0 to 3, but the time field type is BIGINT NOT NULL");

        // add watermark with an undefined nested column as rowtime
        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb1 add (x row<f0 string, f1 timestamp(3)>, watermark for x.f1 as x.f1)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Watermark strategy on nested column is not supported yet.");

        // add watermark to the table which already has watermark defined
        prepareNonManagedTable("tb2", true);

        assertThatThrownBy(() -> parse("alter table tb2 add watermark for ts as ts"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "The base table has already defined the watermark strategy "
                                + "`ts` AS ts - interval '5' seconds. "
                                + "You might want to drop it before adding a new one.");
    }

    @Test
    public void testAlterTableAddWatermark() throws Exception {
        prepareNonManagedTable("tb1", false);

        ObjectIdentifier tableIdentifier = ObjectIdentifier.of("cat1", "db1", "tb1");
        Schema originalSchema =
                catalogManager.getTable(tableIdentifier).get().getTable().getUnresolvedSchema();

        // test add watermark with existed physical column
        Operation operation = parse("alter table tb1 add watermark for ts as ts");
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "ALTER TABLE cat1.db1.tb1 SET SCHEMA (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT NOT NULL,\n"
                                + "  `c` STRING,\n"
                                + "  `d` AS [a*(b+2 + a*b)],\n"
                                + "  `e` ROW<`f0` STRING, `f1` INT, `f2` ROW<`f0` DOUBLE, `f1` ARRAY<FLOAT>>>,\n"
                                + "  `f` AS [e.f1 + e.f2.f0],\n"
                                + "  `g` METADATA VIRTUAL,\n"
                                + "  `ts` TIMESTAMP(3) COMMENT 'just a comment',\n"
                                + "  WATERMARK FOR `ts` AS [`ts`]\n"
                                + ")");
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder().fromSchema(originalSchema).watermark("ts", "`ts`").build());

        // add watermark with new added physical column as rowtime
        operation =
                parse("alter table tb1 add (tss timestamp(3) not null, watermark for tss as tss)");
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "ALTER TABLE cat1.db1.tb1 SET SCHEMA (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT NOT NULL,\n"
                                + "  `c` STRING,\n"
                                + "  `d` AS [a*(b+2 + a*b)],\n"
                                + "  `e` ROW<`f0` STRING, `f1` INT, `f2` ROW<`f0` DOUBLE, `f1` ARRAY<FLOAT>>>,\n"
                                + "  `f` AS [e.f1 + e.f2.f0],\n"
                                + "  `g` METADATA VIRTUAL,\n"
                                + "  `ts` TIMESTAMP(3) COMMENT 'just a comment',\n"
                                + "  `tss` TIMESTAMP(3) NOT NULL,\n"
                                + "  WATERMARK FOR `tss` AS [`tss`]\n"
                                + ")");
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .fromSchema(originalSchema)
                        .column("tss", DataTypes.TIMESTAMP(3).notNull())
                        .watermark("tss", "`tss`")
                        .build());

        // add watermark with new added computed column as rowtime
        operation =
                parse(
                        "alter table tb1 add (log_ts string not null,\n"
                                + "tss as to_timestamp(log_ts),\n"
                                + "watermark for tss as tss - interval '3' second)");
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "ALTER TABLE cat1.db1.tb1 SET SCHEMA (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT NOT NULL,\n"
                                + "  `c` STRING,\n"
                                + "  `d` AS [a*(b+2 + a*b)],\n"
                                + "  `e` ROW<`f0` STRING, `f1` INT, `f2` ROW<`f0` DOUBLE, `f1` ARRAY<FLOAT>>>,\n"
                                + "  `f` AS [e.f1 + e.f2.f0],\n"
                                + "  `g` METADATA VIRTUAL,\n"
                                + "  `ts` TIMESTAMP(3) COMMENT 'just a comment',\n"
                                + "  `log_ts` STRING NOT NULL,\n"
                                + "  `tss` AS [`to_timestamp`(`log_ts`)],\n"
                                + "  WATERMARK FOR `tss` AS [`tss` - INTERVAL '3' SECOND]\n"
                                + ")");
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .fromSchema(originalSchema)
                        .column("log_ts", DataTypes.STRING().notNull())
                        .columnByExpression("tss", "`to_timestamp`(`log_ts`)")
                        .watermark("tss", "`tss` - INTERVAL '3' SECOND")
                        .build());

        // define watermark on computed column which is derived from nested type
        operation =
                parse(
                        "alter table tb1 add (x row<f0 string, f1 timestamp(3) not null> not null, "
                                + "y as x.f1, watermark for y as y - interval '1' day)");
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "ALTER TABLE cat1.db1.tb1 SET SCHEMA (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT NOT NULL,\n"
                                + "  `c` STRING,\n"
                                + "  `d` AS [a*(b+2 + a*b)],\n"
                                + "  `e` ROW<`f0` STRING, `f1` INT, `f2` ROW<`f0` DOUBLE, `f1` ARRAY<FLOAT>>>,\n"
                                + "  `f` AS [e.f1 + e.f2.f0],\n"
                                + "  `g` METADATA VIRTUAL,\n"
                                + "  `ts` TIMESTAMP(3) COMMENT 'just a comment',\n"
                                + "  `x` ROW<`f0` STRING, `f1` TIMESTAMP(3) NOT NULL> NOT NULL,\n"
                                + "  `y` AS [`x`.`f1`],\n"
                                + "  WATERMARK FOR `y` AS [`y` - INTERVAL '1' DAY]\n"
                                + ")");
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .fromSchema(originalSchema)
                        .column(
                                "x",
                                DataTypes.ROW(DataTypes.STRING(), DataTypes.TIMESTAMP(3).notNull())
                                        .notNull())
                        .columnByExpression("y", "`x`.`f1`")
                        .watermark("y", "`y` - INTERVAL '1' DAY")
                        .build());
    }

    @Test
    public void testFailedToAlterTableModifyColumn() throws Exception {
        prepareNonManagedTable("tb1", true);

        // modify duplicated column same
        assertThatThrownBy(() -> parse("alter table tb1 modify (b int, b array<int not null>)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\nEncounter duplicate column `b`.");

        // modify nonexistent column name
        assertThatThrownBy(() -> parse("alter table tb1 modify x bigint"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\nTry to modify a column `x` which does not exist in the table.");

        // refer to nonexistent column name
        assertThatThrownBy(() -> parse("alter table tb1 modify a bigint after x"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\nReferenced column `x` by 'AFTER' does not exist in the table.");

        // modify physical columns which generates computed column
        assertThatThrownBy(() -> parse("alter table tb1 modify e array<int>"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\nInvalid expression for computed column 'f'.");

        assertThatThrownBy(() -> parse("alter table tb1 modify a string"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\nInvalid expression for computed column 'd'.");

        assertThatThrownBy(() -> parse("alter table tb1 modify b as a + 2"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\nInvalid expression for computed column 'd'.");

        assertThatThrownBy(() -> parse("alter table tb1 modify (a timestamp(3), b multiset<int>)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\nInvalid expression for computed column 'd'.");

        // modify the rowtime field which defines watermark
        assertThatThrownBy(() -> parse("alter table tb1 modify ts int"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\nInvalid data type of time field for watermark definition. "
                                + "The field must be of type TIMESTAMP(p) or TIMESTAMP_LTZ(p), "
                                + "the supported precision 'p' is from 0 to 3, but the time field type is INT");

        // modify pk fields
        prepareNonManagedTable("tb2", 1);

        assertThatThrownBy(() -> parse("alter table tb2 modify (d int, a as b + 2)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\nInvalid primary key 'ct1'. Column 'a' is not a physical column.");

        assertThatThrownBy(() -> parse("alter table tb2 modify (d string, a int metadata virtual)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\nInvalid primary key 'ct1'. Column 'a' is not a physical column.");

        // modify an inner field to a nested row
        assertThatThrownBy(() -> parse("alter table tb2 modify (e.f0 string)"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Alter nested row type is not supported yet.");

        // refer to a nested inner field
        assertThatThrownBy(() -> parse("alter table tb2 modify (g string after e.f2)"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Alter nested row type is not supported yet.");

        assertThatThrownBy(() -> parse("alter table tb2 modify (e.f0 string after e.f1)"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Alter nested row type is not supported yet.");
    }

    @Test
    public void testAlterTableModifyColumn() throws Exception {
        prepareNonManagedTable("tb1", 2);

        ObjectIdentifier tableIdentifier = ObjectIdentifier.of("cat1", "db1", "tb1");

        // modify a single column (change pos and add comment)
        Operation operation =
                parse(
                        "alter table tb1 modify b bigint not null comment 'move b to first and add comment' first");
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .column("b", DataTypes.BIGINT().notNull())
                        .withComment("move b to first and add comment")
                        .column("a", DataTypes.INT().notNull())
                        .column("c", DataTypes.STRING())
                        .columnByExpression("d", "a*(b+2 + a*b)")
                        .column(
                                "e",
                                DataTypes.ROW(
                                        DataTypes.STRING(),
                                        DataTypes.INT(),
                                        DataTypes.ROW(
                                                DataTypes.DOUBLE(),
                                                DataTypes.ARRAY(DataTypes.FLOAT()))))
                        .columnByExpression("f", "e.f1 + e.f2.f0")
                        .columnByMetadata("g", DataTypes.STRING(), null, true)
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .withComment("just a comment")
                        .primaryKeyNamed("ct1", "a", "b")
                        .build());

        // change nullability and pos
        operation = parse("alter table tb1 modify ts timestamp(3) not null after e");
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.BIGINT().notNull())
                        .column("c", DataTypes.STRING())
                        .columnByExpression("d", "a*(b+2 + a*b)")
                        .column(
                                "e",
                                DataTypes.ROW(
                                        DataTypes.STRING(),
                                        DataTypes.INT(),
                                        DataTypes.ROW(
                                                DataTypes.DOUBLE(),
                                                DataTypes.ARRAY(DataTypes.FLOAT()))))
                        .column("ts", DataTypes.TIMESTAMP(3).notNull())
                        .withComment("just a comment")
                        .columnByExpression("f", "e.f1 + e.f2.f0")
                        .columnByMetadata("g", DataTypes.STRING(), null, true)
                        .primaryKeyNamed("ct1", "a", "b")
                        .build());

        // modify multiple columns (change pos, nullability, add comment) and pk constraint
        operation =
                parse(
                        "alter table tb1 modify (\n"
                                + "d as a + 2 comment 'change d' after b,\n"
                                + "c bigint first,\n"
                                + "e string comment 'change e',\n"
                                + "f as upper(e) comment 'change f' after ts,\n"
                                + "g int not null comment 'change g',\n"
                                + "constraint ct2 primary key(e) not enforced)");

        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .column("c", DataTypes.BIGINT())
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.BIGINT().notNull())
                        .columnByExpression("d", "`a` + 2")
                        .withComment("change d")
                        .column("e", DataTypes.STRING().notNull())
                        .withComment("change e")
                        .column("g", DataTypes.INT().notNull())
                        .withComment("change g")
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .withComment("just a comment")
                        .columnByExpression("f", "UPPER(`e`)")
                        .withComment("change f")
                        .primaryKeyNamed("ct2", "e")
                        .build());

        // modify multiple columns and watermark spec
        prepareNonManagedTable("tb2", true);
        tableIdentifier = ObjectIdentifier.of("cat1", "db1", "tb2");
        operation =
                parse(
                        "alter table tb2 modify (ts int comment 'change ts',\n"
                                + "f timestamp(3) not null,\n"
                                + "e int metadata virtual,\n"
                                + "watermark for f as f,\n"
                                + "g multiset<int> not null comment 'change g' first)");

        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .column("g", DataTypes.MULTISET(DataTypes.INT()).notNull())
                        .withComment("change g")
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.BIGINT().notNull())
                        .column("c", DataTypes.STRING())
                        .columnByExpression("d", "a*(b+2 + a*b)")
                        .columnByMetadata("e", DataTypes.INT(), null, true)
                        .column("f", DataTypes.TIMESTAMP(3).notNull())
                        .column("ts", DataTypes.INT())
                        .withComment("change ts")
                        .watermark("f", "`f`")
                        .build());
    }

    @Test
    public void testFailedToAlterTableModifyPk() throws Exception {
        prepareNonManagedTable("tb1", 0);

        // modify pk on a table without pk specified
        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb1 modify constraint ct primary key (b) not enforced"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "The base table does not define any primary key constraint. You might want to add a new one.");

        prepareNonManagedTable("tb2", 1);

        // specify a nonexistent column as pk
        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb2 modify constraint ct primary key (x) not enforced"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Invalid primary key 'ct'. Column 'x' does not exist.");

        // specify computed column as pk
        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb2 modify constraint ct primary key (d) not enforced"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Invalid primary key 'ct'. Column 'd' is not a physical column.");

        // specify metadata column as pk
        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb2 modify constraint ct primary key (g) not enforced"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "Invalid primary key 'ct'. Column 'g' is not a physical column.");
    }

    @Test
    public void testAlterTableModifyPk() throws Exception {
        prepareNonManagedTable("tb1", 1);

        // test modify constraint name
        Operation operation =
                parse("alter table tb1 modify constraint ct2 primary key (a, b) not enforced");

        ObjectIdentifier tableIdentifier = ObjectIdentifier.of("cat1", "db1", "tb1");
        Schema originalSchema =
                catalogManager.getTable(tableIdentifier).get().getTable().getUnresolvedSchema();

        assertAlterTableSchema(
                operation,
                ObjectIdentifier.of("cat1", "db1", "tb1"),
                Schema.newBuilder()
                        .fromColumns(originalSchema.getColumns())
                        .primaryKeyNamed("ct2", "a", "b")
                        .build());

        // test modify pk will change column c's nullability
        operation = parse("alter table tb1 modify primary key (c, a) not enforced");
        assertAlterTableSchema(
                operation,
                ObjectIdentifier.of("cat1", "db1", "tb1"),
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.BIGINT().notNull())
                        .column("c", DataTypes.STRING().notNull())
                        .columnByExpression("d", "a*(b+2 + a*b)")
                        .column(
                                "e",
                                DataTypes.ROW(
                                        DataTypes.STRING(),
                                        DataTypes.INT(),
                                        DataTypes.ROW(
                                                DataTypes.DOUBLE(),
                                                DataTypes.ARRAY(DataTypes.FLOAT()))))
                        .columnByExpression("f", "e.f1 + e.f2.f0")
                        .columnByMetadata("g", DataTypes.STRING(), null, true)
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .withComment("just a comment")
                        .primaryKeyNamed("PK_c_a", "c", "a")
                        .build());
    }

    @Test
    public void testFailedToAlterTableModifyWatermark() throws Exception {
        prepareNonManagedTable("tb1", false);

        // modify watermark on a table without watermark
        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb1 modify watermark for a as to_timestamp(a) - interval '1' minute"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\nThe base table does not define any watermark. You might want to add a new one.");

        prepareNonManagedTable("tb2", true);

        // specify invalid watermark spec
        assertThatThrownBy(() -> parse("alter table tb2 modify watermark for a as a"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\nInvalid data type of time field for watermark definition. "
                                + "The field must be of type TIMESTAMP(p) or TIMESTAMP_LTZ(p), the supported precision 'p' is from 0 to 3, "
                                + "but the time field type is INT NOT NULL");

        assertThatThrownBy(
                        () ->
                                parse(
                                        "alter table tb2 modify watermark for c as to_timestamp(c) - interval '1' day"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\nInvalid data type of time field for watermark definition. "
                                + "The field must be of type TIMESTAMP(p) or TIMESTAMP_LTZ(p), the supported precision 'p' is from 0 to 3, "
                                + "but the time field type is STRING");
    }

    @Test
    public void testAlterTableModifyWatermark() throws Exception {
        prepareNonManagedTable("tb1", true);

        // modify watermark offset
        Operation operation = parse("alter table tb1 modify watermark for ts as ts");

        ObjectIdentifier tableIdentifier = ObjectIdentifier.of("cat1", "db1", "tb1");
        Schema originalSchema =
                catalogManager.getTable(tableIdentifier).get().getTable().getUnresolvedSchema();
        List<Schema.UnresolvedColumn> columns = originalSchema.getColumns();
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder().fromColumns(columns).watermark("ts", "`ts`").build());

        // modify watermark rowtime field
        operation = parse("alter table tb1 modify (g timestamp(3) not null, watermark for g as g)");
        assertAlterTableSchema(
                operation,
                tableIdentifier,
                Schema.newBuilder()
                        .fromColumns(columns.subList(0, columns.size() - 2))
                        .column("g", DataTypes.TIMESTAMP(3).notNull())
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .withComment("just a comment")
                        .watermark("g", "`g`")
                        .build());
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

        Operation operation = parse(sql);
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

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(CreateViewOperation.class);
    }

    @Test
    public void testBeginStatementSet() {
        final String sql = "BEGIN STATEMENT SET";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(BeginStatementSetOperation.class);
        final BeginStatementSetOperation beginStatementSetOperation =
                (BeginStatementSetOperation) operation;

        assertThat(beginStatementSetOperation.asSummaryString()).isEqualTo("BEGIN STATEMENT SET");
    }

    @Test
    public void testEnd() {
        final String sql = "END";
        Operation operation = parse(sql);
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
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowJarsOperation.class);
        final ShowJarsOperation showModulesOperation = (ShowJarsOperation) operation;
        assertThat(showModulesOperation.asSummaryString()).isEqualTo("SHOW JARS");
    }

    @Test
    public void testSet() {
        Operation operation1 = parse("SET");
        assertThat(operation1).isInstanceOf(SetOperation.class);
        SetOperation setOperation1 = (SetOperation) operation1;
        assertThat(setOperation1.getKey()).isNotPresent();
        assertThat(setOperation1.getValue()).isNotPresent();

        Operation operation2 = parse("SET 'test-key' = 'test-value'");
        assertThat(operation2).isInstanceOf(SetOperation.class);
        SetOperation setOperation2 = (SetOperation) operation2;
        assertThat(setOperation2.getKey()).hasValue("test-key");
        assertThat(setOperation2.getValue()).hasValue("test-value");
    }

    @Test
    public void testReset() {
        Operation operation1 = parse("RESET");
        assertThat(operation1).isInstanceOf(ResetOperation.class);
        assertThat(((ResetOperation) operation1).getKey()).isNotPresent();

        Operation operation2 = parse("RESET 'test-key'");
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
        assertThat(args).hasSize(2);
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
        Operation operation = parse(sql);
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

    private void assertAlterTableSchema(
            Operation operation, ObjectIdentifier expectedIdentifier, Schema expectedSchema) {
        assertThat(operation).isInstanceOf(AlterTableSchemaOperation.class);
        final AlterTableSchemaOperation alterTableSchemaOperation =
                (AlterTableSchemaOperation) operation;
        assertThat(alterTableSchemaOperation.getTableIdentifier()).isEqualTo(expectedIdentifier);
        assertThat(alterTableSchemaOperation.getCatalogTable().getUnresolvedSchema())
                .isEqualTo(expectedSchema);
    }

    private Operation parse(String sql, FlinkPlannerImpl planner, CalciteParser parser) {
        SqlNode node = parser.parse(sql);
        return SqlToOperationConverter.convert(planner, catalogManager, node).get();
    }

    private Operation parse(String sql) {
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        SqlNode node = parser.parse(sql);
        return SqlToOperationConverter.convert(planner, catalogManager, node).get();
    }

    private void prepareNonManagedTable(boolean hasConstraint) throws Exception {
        prepareNonManagedTable("tb1", hasConstraint ? 1 : 0);
    }

    private void prepareNonManagedTable(String tableName, int numOfPkFields) throws Exception {
        prepareTable(tableName, false, false, false, numOfPkFields);
    }

    private void prepareNonManagedTable(String tableName, boolean hasWatermark) throws Exception {
        prepareTable(tableName, false, false, hasWatermark, 0);
    }

    private void prepareManagedTable(boolean hasPartition) throws Exception {
        TestManagedTableFactory.MANAGED_TABLES.put(
                ObjectIdentifier.of("cat1", "db1", "tb1"), new AtomicReference<>());
        prepareTable("tb1", true, hasPartition, false, 0);
    }

    private void prepareTable(
            String tableName,
            boolean managedTable,
            boolean hasPartition,
            boolean hasWatermark,
            int numOfPkFields)
            throws Exception {
        Catalog catalog = new GenericInMemoryCatalog("default", "default");
        if (!catalogManager.getCatalog("cat1").isPresent()) {
            catalogManager.registerCatalog("cat1", catalog);
        }
        catalog.createDatabase("db1", new CatalogDatabaseImpl(new HashMap<>(), null), true);
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.BIGINT().notNull())
                        .column("c", DataTypes.STRING())
                        .columnByExpression("d", "a*(b+2 + a*b)")
                        .column(
                                "e",
                                DataTypes.ROW(
                                        DataTypes.STRING(),
                                        DataTypes.INT(),
                                        DataTypes.ROW(
                                                DataTypes.DOUBLE(),
                                                DataTypes.ARRAY(DataTypes.FLOAT()))))
                        .columnByExpression("f", "e.f1 + e.f2.f0")
                        .columnByMetadata("g", DataTypes.STRING(), null, true)
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .withComment("just a comment");
        Map<String, String> options = new HashMap<>();
        options.put("k", "v");
        if (!managedTable) {
            options.put("connector", "dummy");
        }
        if (numOfPkFields == 1) {
            builder.primaryKeyNamed("ct1", "a");
        } else if (numOfPkFields == 2) {
            builder.primaryKeyNamed("ct1", "a", "b");
        }

        if (hasWatermark) {
            builder.watermark("ts", "ts - interval '5' seconds");
        }
        CatalogTable catalogTable =
                CatalogTable.of(
                        builder.build(),
                        "a table",
                        hasPartition ? Arrays.asList("b", "c") : Collections.emptyList(),
                        Collections.unmodifiableMap(options));
        catalogManager.setCurrentCatalog("cat1");
        catalogManager.setCurrentDatabase("db1");
        ObjectIdentifier tableIdentifier = ObjectIdentifier.of("cat1", "db1", tableName);
        catalogManager.createTable(catalogTable, tableIdentifier, true);
    }

    private FlinkPlannerImpl getPlannerBySqlDialect(SqlDialect sqlDialect) {
        tableConfig.setSqlDialect(sqlDialect);
        return plannerContext.createFlinkPlanner();
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
