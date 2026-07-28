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

package org.apache.flink.table.planner.runtime.common.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogConnection;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.DefaultConnectionFactory;
import org.apache.flink.table.planner.utils.TestingTableEnvironment;
import org.apache.flink.table.secret.GenericInMemorySecretStore;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** IT cases for connection statements. */
@ExtendWith(ParameterizedTestExtension.class)
class ConnectionITCase extends AbstractTestBase {

    @Parameter public boolean isBatch;

    @Parameters(name = "isBatch: {0}")
    public static List<Boolean> parameters() {
        return List.of(true, false);
    }

    private TableEnvironment tEnv;

    @BeforeEach
    void setup() {
        tEnv =
                TestingTableEnvironment.create(
                        isBatch
                                ? EnvironmentSettings.inBatchMode()
                                : EnvironmentSettings.inStreamingMode(),
                        null,
                        TableConfig.getDefault());
    }

    @TestTemplate
    void testCreateTemporaryConnection() {
        tEnv.executeSql("CREATE TEMPORARY CONNECTION my_conn COMMENT 'hi there' WITH ('k' = 'v')");

        assertThat(catalogManager().getConnection(connectionIdentifier("my_conn")))
                .hasValueSatisfying(
                        connection -> {
                            assertThat(connection.getOptions()).containsOnly(entry("k", "v"));
                            assertThat(connection.getComment()).isEqualTo("hi there");
                        });
    }

    @TestTemplate
    void testCreateTemporaryConnectionRejectsDuplicate() {
        tEnv.executeSql("CREATE TEMPORARY CONNECTION my_conn WITH ('k' = 'v1')");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "CREATE TEMPORARY CONNECTION my_conn WITH ('k' = 'v2')"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Temporary connection");

        tEnv.executeSql("CREATE TEMPORARY CONNECTION IF NOT EXISTS my_conn WITH ('k' = 'v2')");

        assertThat(catalogManager().getConnection(connectionIdentifier("my_conn")))
                .hasValueSatisfying(
                        connection ->
                                assertThat(connection.getOptions()).containsOnly(entry("k", "v1")));
    }

    @TestTemplate
    void testCreatePermanentConnectionRejectedWithoutSecretStore() {
        assertThatThrownBy(() -> tEnv.executeSql("CREATE CONNECTION my_conn WITH ('k' = 'v')"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("WritableSecretStore must be configured");
    }

    @TestTemplate
    void testAlterConnectionRenameTemporaryConnection() {
        tEnv.executeSql("CREATE TEMPORARY CONNECTION my_conn WITH ('k' = 'v')");

        tEnv.executeSql("ALTER CONNECTION my_conn RENAME TO new_conn");

        assertThat(catalogManager().getConnection(connectionIdentifier("my_conn"))).isEmpty();
        assertThat(catalogManager().getConnection(connectionIdentifier("new_conn")))
                .hasValueSatisfying(
                        connection ->
                                assertThat(connection.getOptions()).containsOnly(entry("k", "v")));
    }

    @TestTemplate
    void testAlterConnectionRenameIfExists() {
        tEnv.executeSql("ALTER CONNECTION IF EXISTS missing_conn RENAME TO new_conn");

        assertThat(catalogManager().getConnection(connectionIdentifier("new_conn"))).isEmpty();
    }

    @TestTemplate
    void testAlterConnectionRenameToExistingConnectionRejected() {
        tEnv.executeSql("CREATE TEMPORARY CONNECTION my_conn WITH ('k' = 'v')");
        tEnv.executeSql("CREATE TEMPORARY CONNECTION new_conn WITH ('k' = 'v')");

        assertThatThrownBy(() -> tEnv.executeSql("ALTER CONNECTION my_conn RENAME TO new_conn"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("already exists");
    }

    @TestTemplate
    void testAlterTemporaryConnectionRenameToExistingPermanentConnectionRejected() {
        TableEnvironment tableEnv = tableEnvWithSecretStore();
        tableEnv.executeSql("CREATE TEMPORARY CONNECTION my_conn WITH ('k' = 'v')");
        tableEnv.executeSql("CREATE CONNECTION new_conn WITH ('k' = 'v')");

        assertThatThrownBy(() -> tableEnv.executeSql("ALTER CONNECTION my_conn RENAME TO new_conn"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("already exists");
    }

    @TestTemplate
    void testAlterPermanentConnectionRenameToExistingTemporaryConnectionRejected() {
        TableEnvironment tableEnv = tableEnvWithSecretStore();
        tableEnv.executeSql("CREATE CONNECTION my_conn WITH ('k' = 'v')");
        tableEnv.executeSql("CREATE TEMPORARY CONNECTION new_conn WITH ('k' = 'v')");

        assertThatThrownBy(() -> tableEnv.executeSql("ALTER CONNECTION my_conn RENAME TO new_conn"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("already exists");
    }

    private TableEnvironment tableEnvWithSecretStore() {
        final EnvironmentSettings.Builder settings = EnvironmentSettings.newInstance();
        if (isBatch) {
            settings.inBatchMode();
        }
        return TableEnvironment.create(
                settings.withSecretStore(new GenericInMemorySecretStore()).build());
    }

    @TestTemplate
    void testShowCreatePermanentConnection() throws Exception {
        ObjectIdentifier identifier = connectionIdentifier("my_conn");
        catalogManager()
                .getCatalog(catalogManager().getCurrentCatalog())
                .orElseThrow()
                .createConnection(
                        identifier.toObjectPath(),
                        CatalogConnection.of(Map.of("k", "v", "type", "default"), "hi there"),
                        false);

        List<Row> rows = collectRows("SHOW CREATE CONNECTION my_conn");

        assertThat(rows).hasSize(1);
        String showCreate = (String) rows.get(0).getField(0);
        assertThat(showCreate)
                .contains("CREATE CONNECTION")
                .doesNotContain("CREATE TEMPORARY CONNECTION")
                .contains("`my_conn`")
                .contains("COMMENT 'hi there'")
                .contains("'k' = 'v'")
                .contains("'type' = 'default'");
    }

    @TestTemplate
    void testShowCreateTemporaryConnection() throws Exception {
        tEnv.executeSql(
                "CREATE TEMPORARY CONNECTION my_conn COMMENT 'hi there' "
                        + "WITH ('type' = 'default', 'k' = 'v', 'password' = 'super-secret')");

        assertThat(catalogManager().getConnection(connectionIdentifier("my_conn")))
                .hasValueSatisfying(
                        connection ->
                                assertThat(connection.getOptions())
                                        .containsKeys(
                                                "k",
                                                "type",
                                                DefaultConnectionFactory.SECRET_REFERENCE_KEY)
                                        .doesNotContainKey("password"));

        List<Row> rows = collectRows("SHOW CREATE CONNECTION my_conn");

        assertThat(rows).hasSize(1);
        String showCreate = (String) rows.get(0).getField(0);
        assertThat(showCreate)
                .contains("CREATE TEMPORARY CONNECTION")
                .contains("`my_conn`")
                .contains("COMMENT 'hi there'")
                .contains("'k' = 'v'")
                .contains("'type' = 'default'")
                .doesNotContain("super-secret")
                .doesNotContain("password")
                .doesNotContain(DefaultConnectionFactory.SECRET_REFERENCE_KEY);
    }

    @TestTemplate
    void testShowCreateSecretOnlyTemporaryConnection() throws Exception {
        tEnv.executeSql("CREATE TEMPORARY CONNECTION my_conn WITH ('password' = 'super-secret')");

        List<Row> rows = collectRows("SHOW CREATE CONNECTION my_conn");

        assertThat(rows).hasSize(1);
        String showCreate = (String) rows.get(0).getField(0);
        assertThat(showCreate)
                .contains("CREATE TEMPORARY CONNECTION")
                .contains("WITH (\n  'type' = 'default'\n)\n")
                .doesNotContain("password")
                .doesNotContain("super-secret")
                .doesNotContain(DefaultConnectionFactory.SECRET_REFERENCE_KEY);

        catalogManager().dropTemporaryConnection(connectionIdentifier("my_conn"), false);
        tEnv.executeSql(showCreate);

        assertThat(catalogManager().getConnection(connectionIdentifier("my_conn")))
                .hasValueSatisfying(
                        connection ->
                                assertThat(connection.getOptions())
                                        .containsOnly(entry("type", "default")));
    }

    @TestTemplate
    void testShowConnections() throws Exception {
        ObjectIdentifier identifier = connectionIdentifier("permanent_conn");
        catalogManager()
                .getCatalog(identifier.getCatalogName())
                .orElseThrow()
                .createConnection(
                        identifier.toObjectPath(),
                        CatalogConnection.of(Map.of("k", "v"), null),
                        false);
        tEnv.executeSql("CREATE TEMPORARY CONNECTION b_conn WITH ('k' = 'v')");
        tEnv.executeSql("CREATE TEMPORARY CONNECTION a_conn WITH ('k' = 'v')");

        assertThat(collectRows("SHOW CONNECTIONS"))
                .containsExactly(Row.of("a_conn"), Row.of("b_conn"), Row.of("permanent_conn"));
        assertThat(collectRows("SHOW CONNECTIONS FROM " + identifier.getDatabaseName()))
                .containsExactly(Row.of("a_conn"), Row.of("b_conn"), Row.of("permanent_conn"));
    }

    @TestTemplate
    void testShowConnectionsInDatabase() throws Exception {
        tEnv.executeSql("CREATE DATABASE other_db");
        tEnv.executeSql("CREATE TEMPORARY CONNECTION current_conn WITH ('k' = 'v')");
        tEnv.executeSql("CREATE TEMPORARY CONNECTION other_db.temporary_conn WITH ('k' = 'v')");
        catalogManager()
                .getCatalog(catalogManager().getCurrentCatalog())
                .orElseThrow()
                .createConnection(
                        ObjectIdentifier.of(
                                        catalogManager().getCurrentCatalog(),
                                        "other_db",
                                        "permanent_conn")
                                .toObjectPath(),
                        CatalogConnection.of(Map.of("k", "v"), null),
                        false);

        for (String preposition : List.of("IN", "FROM")) {
            for (String database :
                    List.of("other_db", catalogManager().getCurrentCatalog() + ".other_db")) {
                assertThat(collectRows("SHOW CONNECTIONS " + preposition + " " + database))
                        .containsExactly(Row.of("permanent_conn"), Row.of("temporary_conn"));
            }
        }
        assertThat(collectRows("SHOW CONNECTIONS")).containsExactly(Row.of("current_conn"));
    }

    @TestTemplate
    void testShowConnectionsLike() throws Exception {
        tEnv.executeSql("CREATE TEMPORARY CONNECTION prod_conn WITH ('k' = 'v')");
        tEnv.executeSql("CREATE TEMPORARY CONNECTION tmp_conn WITH ('k' = 'v')");

        assertThat(collectRows("SHOW CONNECTIONS LIKE 'prod_%'"))
                .containsExactly(Row.of("prod_conn"));
        assertThat(collectRows("SHOW CONNECTIONS NOT LIKE 'prod_%'"))
                .containsExactly(Row.of("tmp_conn"));
    }

    @TestTemplate
    void testShowConnectionsRejectsMissingDatabase() {
        assertThatThrownBy(() -> tEnv.executeSql("SHOW CONNECTIONS FROM missing_db"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Database 'missing_db' does not exist in catalog '%s'.",
                        catalogManager().getCurrentCatalog());
    }

    @TestTemplate
    void testShowConnectionsRejectsMissingCatalog() {
        assertThatThrownBy(() -> tEnv.executeSql("SHOW CONNECTIONS IN missing_cat.db"))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Catalog missing_cat does not exist");
    }

    @TestTemplate
    void testShowConnectionsRejectsInvalidDatabaseIdentifier() {
        assertThatThrownBy(() -> tEnv.executeSql("SHOW CONNECTIONS FROM a.b.c"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "SHOW CONNECTIONS from/in identifier [ a.b.c ] format error, it should be [catalog_name.]database_name.");
    }

    @TestTemplate
    void testDescribeTemporaryConnection() throws Exception {
        tEnv.executeSql(
                "CREATE TEMPORARY CONNECTION my_conn COMMENT 'hi there' "
                        + "WITH ('type' = 'default', 'k' = 'v', 'comment' = 'option comment', "
                        + "'password' = 'super-secret')");

        List<Row> rows = collectRows("DESCRIBE CONNECTION my_conn");

        assertThat(rows)
                .containsExactly(
                        Row.of("type", "default"),
                        Row.of("option:comment", "option comment"),
                        Row.of("option:k", "v"),
                        Row.of("comment", "hi there"),
                        Row.of("temporary", "true"));
    }

    @TestTemplate
    void testDescribeSecretOnlyConnectionIncludesDefaultType() throws Exception {
        tEnv.executeSql("CREATE TEMPORARY CONNECTION my_conn WITH ('password' = 'secret')");

        assertThat(collectRows("DESCRIBE CONNECTION my_conn"))
                .containsExactly(Row.of("type", "default"), Row.of("temporary", "true"));
    }

    @TestTemplate
    void testDescribePermanentConnectionIncludesScope() throws Exception {
        ObjectIdentifier identifier = connectionIdentifier("my_conn");
        catalogManager()
                .getCatalog(catalogManager().getCurrentCatalog())
                .orElseThrow()
                .createConnection(
                        identifier.toObjectPath(),
                        CatalogConnection.of(Map.of("k", "v"), null),
                        false);

        assertThat(collectRows("DESCRIBE CONNECTION my_conn"))
                .containsExactly(
                        Row.of("type", "default"),
                        Row.of("option:k", "v"),
                        Row.of("temporary", "false"));
    }

    @TestTemplate
    void testDescribeMissingConnectionRejected() {
        assertThatThrownBy(() -> tEnv.executeSql("DESCRIBE CONNECTION missing_conn"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Connection with identifier");
    }

    private List<Row> collectRows(String sql) throws Exception {
        TableResult result = tEnv.executeSql(sql);
        try (CloseableIterator<Row> rows = result.collect()) {
            return CollectionUtil.iteratorToList(rows);
        }
    }

    private CatalogManager catalogManager() {
        return ((TableEnvironmentInternal) tEnv).getCatalogManager();
    }

    private ObjectIdentifier connectionIdentifier(String connectionName) {
        CatalogManager catalogManager = catalogManager();
        return ObjectIdentifier.of(
                catalogManager.getCurrentCatalog(),
                catalogManager.getCurrentDatabase(),
                connectionName);
    }
}
