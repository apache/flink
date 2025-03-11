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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ModelChange;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.DescribeModelOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ShowModelsOperation;
import org.apache.flink.table.operations.ddl.AlterModelChangeOperation;
import org.apache.flink.table.operations.ddl.AlterModelRenameOperation;
import org.apache.flink.table.operations.ddl.CreateModelOperation;
import org.apache.flink.table.operations.ddl.DropModelOperation;
import org.apache.flink.table.operations.utils.LikeType;
import org.apache.flink.table.operations.utils.ShowLikeOperator;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.parse.CalciteParser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Test for testing convert model statement to operation. */
public class SqlModelOperationConverterTest extends SqlNodeToOperationConversionTestBase {
    @Test
    public void testCreateModel() {
        final String sql =
                "CREATE MODEL model1 \n"
                        + "INPUT(a bigint comment 'column a', b varchar, c int, d varchar)\n"
                        + "OUTPUT(e bigint, f int)\n"
                        + "  with (\n"
                        + "    'task' = 'clustering',\n"
                        + "    'provider' = 'openai',\n"
                        + "    'openai.endpoint' = 'someendpoint'\n"
                        + ")\n";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(CreateModelOperation.class);
        CreateModelOperation op = (CreateModelOperation) operation;
        CatalogModel catalogModel = op.getCatalogModel();
        assertThat(catalogModel.getOptions())
                .isEqualTo(
                        Map.of(
                                "provider",
                                "openai",
                                "openai.endpoint",
                                "someendpoint",
                                "task",
                                "clustering"));
        Schema inputSchema = catalogModel.getInputSchema();
        assertNotNull(inputSchema);
        assertThat(
                        inputSchema.getColumns().stream()
                                .map(Schema.UnresolvedColumn::getName)
                                .collect(Collectors.toList()))
                .isEqualTo(Arrays.asList("a", "b", "c", "d"));
        Schema outputSchema = catalogModel.getOutputSchema();
        assertNotNull(outputSchema);
        assertThat(
                        outputSchema.getColumns().stream()
                                .map(Schema.UnresolvedColumn::getName)
                                .collect(Collectors.toList()))
                .isEqualTo(Arrays.asList("e", "f"));
    }

    @Test
    public void testDropModel() throws Exception {
        Catalog catalog = new GenericInMemoryCatalog("default", "default");
        if (!catalogManager.getCatalog("cat1").isPresent()) {
            catalogManager.registerCatalog("cat1", catalog);
        }
        catalogManager.createDatabase(
                "cat1", "db1", new CatalogDatabaseImpl(new HashMap<>(), null), true);
        prepareModel();

        String[] dropModelSqls =
                new String[] {
                    "DROP MODEL cat1.db1.m1",
                    "DROP MODEL db1.m1",
                    "DROP MODEL m1",
                    "DROP MODEL IF EXISTS m2"
                };
        ObjectIdentifier m1Id = ObjectIdentifier.of("cat1", "db1", "m1");
        ObjectIdentifier m2Id = ObjectIdentifier.of("cat1", "db1", "m2");
        ObjectIdentifier[] expectedIdentifiers =
                new ObjectIdentifier[] {m1Id, m1Id, m1Id, m2Id, m2Id};
        boolean[] expectedIfExists = new boolean[] {false, false, false, true};

        for (int i = 0; i < dropModelSqls.length; i++) {
            String sql = dropModelSqls[i];
            Operation operation = parse(sql);
            assertThat(operation).isInstanceOf(DropModelOperation.class);
            final DropModelOperation dropModelOperation = (DropModelOperation) operation;
            assertThat(dropModelOperation.getModelIdentifier()).isEqualTo(expectedIdentifiers[i]);
            assertThat(dropModelOperation.isIfExists()).isEqualTo(expectedIfExists[i]);
        }
    }

    @Test
    public void testDescribeModel() {
        Operation operation = parse("DESCRIBE MODEL m1");
        assertThat(operation).isInstanceOf(DescribeModelOperation.class);
        DescribeModelOperation describeModelOperation = (DescribeModelOperation) operation;
        assertThat(describeModelOperation.getSqlIdentifier())
                .isEqualTo(ObjectIdentifier.of("builtin", "default", "m1"));
        assertThat(describeModelOperation.isExtended()).isFalse();

        operation = parse("DESCRIBE MODEL EXTENDED m1");
        assertThat(operation).isInstanceOf(DescribeModelOperation.class);
        describeModelOperation = (DescribeModelOperation) operation;
        assertThat(describeModelOperation.isExtended()).isTrue();
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("inputForShowModelsTest")
    void testShowModels(String sql, ShowModelsOperation expected, String expectedSummary) {
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowModelsOperation.class).isEqualTo(expected);
        assertThat(operation.asSummaryString()).isEqualTo(expectedSummary);
    }

    private static Stream<Arguments> inputForShowModelsTest() {
        return Stream.of(
                Arguments.of(
                        "SHOW MODELS FROM db1",
                        new ShowModelsOperation("builtin", "db1", "FROM", null),
                        "SHOW MODELS FROM builtin.db1"),
                Arguments.of(
                        "SHOW MODELS",
                        new ShowModelsOperation("builtin", "db1", null, null),
                        "SHOW MODELS"),
                Arguments.of(
                        "SHOW MODELS FROM `builtin`.`db1` LIKE '%abc%'",
                        new ShowModelsOperation(
                                "builtin",
                                "db1",
                                "FROM",
                                ShowLikeOperator.of(LikeType.LIKE, "%abc%")),
                        "SHOW MODELS FROM builtin.db1 LIKE '%abc%'"),
                Arguments.of(
                        "SHOW MODELS FROM `builtin`.`db1` NOT LIKE '%abc%'",
                        new ShowModelsOperation(
                                "builtin",
                                "db1",
                                "FROM",
                                ShowLikeOperator.of(LikeType.NOT_LIKE, "%abc%")),
                        "SHOW MODELS FROM builtin.db1 NOT LIKE '%abc%'"));
    }

    private void prepareModel() {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();
        Schema outputSchema = Schema.newBuilder().column("label", DataTypes.STRING()).build();
        Map<String, String> properties = new HashMap<>();
        properties.put("K1", "v1");
        CatalogModel catalogModel = CatalogModel.of(inputSchema, outputSchema, properties, null);
        catalogManager.setCurrentCatalog("cat1");
        catalogManager.setCurrentDatabase("db1");
        ObjectIdentifier modelIdentifier = ObjectIdentifier.of("cat1", "db1", "m1");
        catalogManager.createModel(catalogModel, modelIdentifier, true);
    }

    @Test
    public void testAlterModel() throws Exception {
        Catalog catalog = new GenericInMemoryCatalog("default", "default");
        if (!catalogManager.getCatalog("cat1").isPresent()) {
            catalogManager.registerCatalog("cat1", catalog);
        }
        catalogManager.createDatabase(
                "cat1", "db1", new CatalogDatabaseImpl(new HashMap<>(), null), true);
        prepareModel();

        final String[] renameModelSqls =
                new String[] {
                    "ALTER MODEL cat1.db1.m1 RENAME to m2",
                    "ALTER MODEL db1.m1 RENAME to m2",
                    "ALTER MODEL m1 RENAME to cat1.db1.m2",
                };
        final ObjectIdentifier expectedIdentifier = ObjectIdentifier.of("cat1", "db1", "m1");
        final ObjectIdentifier expectedNewIdentifier = ObjectIdentifier.of("cat1", "db1", "m2");
        // test rename table converter
        for (String renameModelSql : renameModelSqls) {
            Operation operation = parse(renameModelSql);
            assertThat(operation).isInstanceOf(AlterModelRenameOperation.class);
            final AlterModelRenameOperation alterModelRenameOperation =
                    (AlterModelRenameOperation) operation;
            assertThat(alterModelRenameOperation.getModelIdentifier())
                    .isEqualTo(expectedIdentifier);
            assertThat(alterModelRenameOperation.getNewModelIdentifier())
                    .isEqualTo(expectedNewIdentifier);
        }

        // test alter model properties with existing key: 'k1'
        Operation operation =
                parse("ALTER MODEL IF EXISTS cat1.db1.m1 SET('k1' = 'v1_altered', 'K2' = 'V2')");
        assertThat(operation).isInstanceOf(AlterModelChangeOperation.class);
        AlterModelChangeOperation alterModelPropertiesOperation =
                (AlterModelChangeOperation) operation;
        assertThat(alterModelPropertiesOperation.getModelIdentifier())
                .isEqualTo(expectedIdentifier);
        List<ModelChange> expectedModelChanges = new ArrayList<>();
        expectedModelChanges.add(ModelChange.set("k1", "v1_altered"));
        expectedModelChanges.add(ModelChange.set("K2", "V2"));
        assertThat(alterModelPropertiesOperation.getModelChanges()).isEqualTo(expectedModelChanges);

        // test alter model properties without keys
        operation = parse("ALTER MODEL IF EXISTS cat1.db1.m1 SET('k3' = 'v3')");
        expectedModelChanges.clear();
        expectedModelChanges.add(ModelChange.set("k3", "v3"));

        assertThat(operation).isInstanceOf(AlterModelChangeOperation.class);
        alterModelPropertiesOperation = (AlterModelChangeOperation) operation;
        assertThat(alterModelPropertiesOperation.getModelIdentifier())
                .isEqualTo(expectedIdentifier);
        assertThat(alterModelPropertiesOperation.getModelChanges()).isEqualTo(expectedModelChanges);

        // test alter model properties empty option not supported
        assertThatThrownBy(() -> parse("ALTER MODEL IF EXISTS cat1.db1.m1 SET()"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("ALTER MODEL SET does not support empty option.");

        // test alter model reset properties with existing key: 'k1'
        operation = parse("ALTER MODEL IF EXISTS cat1.db1.m1 RESET('k1')");
        assertThat(operation).isInstanceOf(AlterModelChangeOperation.class);
        alterModelPropertiesOperation = (AlterModelChangeOperation) operation;
        assertThat(alterModelPropertiesOperation.getModelIdentifier())
                .isEqualTo(expectedIdentifier);
        expectedModelChanges.clear();
        expectedModelChanges.add(ModelChange.reset("k1"));
        assertThat(alterModelPropertiesOperation.getModelChanges()).isEqualTo(expectedModelChanges);

        // test alter model reset properties empty key not supported
        assertThatThrownBy(() -> parse("ALTER MODEL IF EXISTS cat1.db1.m1 RESET()"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("ALTER MODEL RESET does not support empty key.");
    }
}
