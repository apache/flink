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

package org.apache.flink.table.api;

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.ModelNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.operations.SourceQueryOperation;
import org.apache.flink.table.utils.TableEnvironmentMock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

/** Tests for {@link TableEnvironment}. */
class TableEnvironmentTest {
    private TableEnvironmentMock tEnv;
    private static final Schema TEST_SCHEMA =
            Schema.newBuilder().column("f0", DataTypes.INT()).build();
    private static final Schema TEST_SCHEMA_2 =
            Schema.newBuilder().column("f1", DataTypes.INT()).build();
    private static final TableDescriptor TEST_DESCRIPTOR =
            TableDescriptor.forConnector("fake").schema(TEST_SCHEMA).option("a", "Test").build();
    private static final ModelDescriptor TEST_MODEL_DESCRIPTOR =
            ModelDescriptor.forProvider("TestProvider")
                    .option("a", "Test")
                    .inputSchema(TEST_SCHEMA)
                    .outputSchema(TEST_SCHEMA)
                    .build();
    private static final ModelDescriptor TEST_MODEL_DESCRIPTOR_2 =
            ModelDescriptor.forProvider("TestProvider")
                    .option("a", "Test")
                    .inputSchema(TEST_SCHEMA)
                    .outputSchema(TEST_SCHEMA_2)
                    .build();

    private static Stream<Arguments> getModelNamesAndDescriptors() {
        return Stream.of(
                Arguments.of("M", TEST_MODEL_DESCRIPTOR),
                Arguments.of("M2", TEST_MODEL_DESCRIPTOR_2));
    }

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironmentMock.getStreamingInstance();
    }

    @Test
    void testCreateTemporaryTableFromDescriptor() {
        assertTemporaryCreateTableFromDescriptor(tEnv, TEST_SCHEMA, false);
    }

    @Test
    void testCreateTemporaryTableIfNotExistsFromDescriptor() {
        assertTemporaryCreateTableFromDescriptor(tEnv, TEST_SCHEMA, true);
        assertThatNoException()
                .isThrownBy(() -> tEnv.createTemporaryTable("T", TEST_DESCRIPTOR, true));

        assertThatThrownBy(() -> tEnv.createTemporaryTable("T", TEST_DESCRIPTOR, false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Temporary table '`default_catalog`.`default_database`.`T`' already exists");
    }

    @Test
    void testCreateTableFromDescriptor() throws Exception {
        assertCreateTableFromDescriptor(tEnv, TEST_SCHEMA, false);
    }

    @ParameterizedTest(name = "{index}: ignoreIfExists ({0})")
    @ValueSource(booleans = {true, false})
    void testCreateViewFromTable(final boolean ignoreIfExists) throws Exception {
        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        tEnv.createTable("T", TEST_DESCRIPTOR);

        assertThat(tEnv.createView("V", tEnv.from("T"), ignoreIfExists)).isTrue();

        final ObjectPath objectPath = new ObjectPath(database, "V");

        final CatalogBaseTable catalogView =
                tEnv.getCatalog(catalog).orElseThrow(AssertionError::new).getTable(objectPath);
        assertThat(catalogView).isInstanceOf(CatalogView.class);
        assertThat(catalogView.getUnresolvedSchema()).isEqualTo(TEST_SCHEMA);
    }

    @Test
    void testCreateViewWithSameNameIgnoreIfExists() {
        tEnv.createTable("T", TEST_DESCRIPTOR);
        tEnv.createView("V", tEnv.from("T"));

        assertThat(tEnv.createView("V", tEnv.from("T"), true)).isFalse();
    }

    @Test
    void testCreateViewWithSameName() {
        tEnv.createTable("T", TEST_DESCRIPTOR);
        tEnv.createView("V", tEnv.from("T"));

        assertThatThrownBy(() -> tEnv.createView("V", tEnv.from("T"), false))
                .hasCauseInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining(
                        "Could not execute CreateTable in path `default_catalog`.`default_database`.`V`");

        assertThatThrownBy(() -> tEnv.createView("V", tEnv.from("T")))
                .hasCauseInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining(
                        "Could not execute CreateTable in path `default_catalog`.`default_database`.`V`");
    }

    @Test
    void testCreateTableIfNotExistsFromDescriptor() throws Exception {
        assertCreateTableFromDescriptor(tEnv, TEST_SCHEMA, true);
        assertThatNoException().isThrownBy(() -> tEnv.createTable("T", TEST_DESCRIPTOR, true));

        assertThatThrownBy(() -> tEnv.createTable("T", TEST_DESCRIPTOR, false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Could not execute CreateTable in path `default_catalog`.`default_database`.`T`");
    }

    @Test
    void testTableFromDescriptor() {
        final Table table = tEnv.from(TEST_DESCRIPTOR);

        assertThat(Schema.newBuilder().fromResolvedSchema(table.getResolvedSchema()).build())
                .isEqualTo(TEST_SCHEMA);

        assertThat(table.getQueryOperation())
                .asInstanceOf(type(SourceQueryOperation.class))
                .extracting(SourceQueryOperation::getContextResolvedTable)
                .satisfies(
                        crs -> {
                            assertThat(crs.isAnonymous()).isTrue();
                            assertThat(crs.getIdentifier().toList()).hasSize(1);
                            assertThat(crs.getResolvedTable().getOptions())
                                    .containsEntry("connector", "fake");
                        });

        assertThat(tEnv.getCatalogManager().listTables()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("getModelNamesAndDescriptors")
    void testCreateModelFromDescriptor(String modelPath, ModelDescriptor modelDescriptor)
            throws Exception {
        assertCreateModelFromDescriptor(tEnv, modelPath, modelDescriptor);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCreateModelWithSameNameWithIgnoreIfExists(boolean ignoreIfExists) throws Exception {
        assertCreateModelFromDescriptor(tEnv, "M", TEST_MODEL_DESCRIPTOR);

        if (ignoreIfExists) {
            assertThatNoException()
                    .isThrownBy(() -> tEnv.createModel("M", TEST_MODEL_DESCRIPTOR, true));
        } else {
            assertThatThrownBy(() -> tEnv.createModel("M", TEST_MODEL_DESCRIPTOR, false))
                    .isInstanceOf(ValidationException.class)
                    .hasMessage(
                            "Could not execute CreateModel in path `default_catalog`.`default_database`.`M`");
        }
        assertThatThrownBy(() -> tEnv.createModel("M", TEST_MODEL_DESCRIPTOR))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Could not execute CreateModel in path `default_catalog`.`default_database`.`M`");
    }

    @Test
    void testDropModel() throws Exception {
        tEnv.createModel("M", TEST_MODEL_DESCRIPTOR);

        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();
        final ObjectPath objectPath = new ObjectPath(database, "M");
        CatalogModel catalogModel =
                tEnv.getCatalog(catalog).orElseThrow(AssertionError::new).getModel(objectPath);
        assertThat(catalogModel).isInstanceOf(CatalogModel.class);
        assertThat(tEnv.dropModel("M", true)).isTrue();
        assertThatThrownBy(
                        () ->
                                tEnv.getCatalog(catalog)
                                        .orElseThrow(AssertionError::new)
                                        .getModel(objectPath))
                .isInstanceOf(ModelNotExistException.class)
                .hasMessage("Model '`default_catalog`.`default_database`.`M`' does not exist.");
    }

    @Test
    void testNonExistingDropModel() throws Exception {
        assertThat(tEnv.dropModel("M", true)).isFalse();

        assertThatThrownBy(() -> tEnv.dropModel("M", false))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Model with identifier 'default_catalog.default_database.M' does not exist.");
    }

    @ParameterizedTest
    @MethodSource("getModelNamesAndDescriptors")
    void testCreateTemporaryModelFromDescriptor(String modelPath, ModelDescriptor modelDescriptor) {
        assertTemporaryCreateModelFromDescriptor(tEnv, modelPath, modelDescriptor);
        assertThatNoException()
                .isThrownBy(() -> tEnv.createTemporaryModel(modelPath, modelDescriptor, true));

        assertThatThrownBy(() -> tEnv.createTemporaryModel(modelPath, modelDescriptor, false))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        String.format(
                                "Temporary model '`default_catalog`.`default_database`.`%s`' already exists",
                                modelPath));
    }

    @Test
    void testListModels() {
        tEnv.createModel("M1", TEST_MODEL_DESCRIPTOR);
        tEnv.createModel("M2", TEST_MODEL_DESCRIPTOR);

        assertThat(tEnv.listModels()).containsExactly("M1", "M2");
    }

    private static void assertCreateTableFromDescriptor(
            TableEnvironmentMock tEnv, Schema schema, boolean ignoreIfExists)
            throws org.apache.flink.table.catalog.exceptions.TableNotExistException {
        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        if (ignoreIfExists) {
            assertThat(tEnv.createTable("T", TEST_DESCRIPTOR, true)).isTrue();
        } else {
            tEnv.createTable("T", TEST_DESCRIPTOR);
        }

        final ObjectPath objectPath = new ObjectPath(database, "T");
        assertThat(
                        tEnv.getCatalog(catalog)
                                .orElseThrow(AssertionError::new)
                                .tableExists(objectPath))
                .isTrue();

        final CatalogBaseTable catalogTable =
                tEnv.getCatalog(catalog).orElseThrow(AssertionError::new).getTable(objectPath);
        assertThat(catalogTable).isInstanceOf(CatalogTable.class);
        assertThat(catalogTable.getUnresolvedSchema()).isEqualTo(schema);
        assertThat(catalogTable.getOptions())
                .contains(entry("connector", "fake"), entry("a", "Test"));
    }

    private static void assertTemporaryCreateTableFromDescriptor(
            TableEnvironmentMock tEnv, Schema schema, boolean ignoreIfExists) {
        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        if (ignoreIfExists) {
            tEnv.createTemporaryTable("T", TEST_DESCRIPTOR, true);
        } else {
            tEnv.createTemporaryTable("T", TEST_DESCRIPTOR);
        }

        assertThat(
                        tEnv.getCatalog(catalog)
                                .orElseThrow(AssertionError::new)
                                .tableExists(new ObjectPath(database, "T")))
                .isFalse();

        final Optional<ContextResolvedTable> lookupResult =
                tEnv.getCatalogManager().getTable(ObjectIdentifier.of(catalog, database, "T"));
        assertThat(lookupResult).isPresent();

        final CatalogBaseTable catalogTable = lookupResult.get().getResolvedTable();
        assertThat(catalogTable instanceof CatalogTable).isTrue();
        assertThat(catalogTable.getUnresolvedSchema()).isEqualTo(schema);
        assertThat(catalogTable.getOptions().get("connector")).isEqualTo("fake");
        assertThat(catalogTable.getOptions().get("a")).isEqualTo("Test");
    }

    private static void assertCreateModelFromDescriptor(
            TableEnvironmentMock tEnv, String modelPath, ModelDescriptor modelDescriptor)
            throws ModelNotExistException {
        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        tEnv.createModel(modelPath, modelDescriptor, true);
        final ObjectPath objectPath = new ObjectPath(database, modelPath);
        CatalogModel catalogModel =
                tEnv.getCatalog(catalog).orElseThrow(AssertionError::new).getModel(objectPath);
        assertCatalogModelWithModelDescriptor(catalogModel, modelDescriptor);
    }

    private static void assertTemporaryCreateModelFromDescriptor(
            TableEnvironmentMock tEnv, String modelPath, ModelDescriptor modelDescriptor) {
        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        tEnv.createTemporaryModel(modelPath, modelDescriptor, true);
        final Optional<ContextResolvedModel> lookupResult =
                tEnv.getCatalogManager()
                        .getModel(ObjectIdentifier.of(catalog, database, modelPath));
        assertThat(lookupResult).isPresent();
        CatalogModel catalogModel = lookupResult.get().getResolvedModel();
        assertCatalogModelWithModelDescriptor(catalogModel, modelDescriptor);
    }

    private static void assertCatalogModelWithModelDescriptor(
            CatalogModel catalogModel, ModelDescriptor modelDescriptor) {
        assertThat(catalogModel).isNotNull();
        assertThat(catalogModel).isInstanceOf(CatalogModel.class);
        assertThat(catalogModel.getInputSchema()).isEqualTo(modelDescriptor.getInputSchema().get());
        assertThat(catalogModel.getOutputSchema())
                .isEqualTo(modelDescriptor.getOutputSchema().get());
        for (Map.Entry<String, String> entry : modelDescriptor.getOptions().entrySet()) {
            assertThat(catalogModel.getOptions()).contains(entry);
            assertThat(catalogModel.getOptions()).containsEntry(entry.getKey(), entry.getValue());
        }
    }
}
