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
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
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
class ConnectionITCase {

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
        tEnv.executeSql(
                "CREATE TEMPORARY CONNECTION my_conn COMMENT 'hi there' " + "WITH ('k' = 'v')");

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
    void testShowCreateTemporaryConnection() {
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
    void testShowCreateSecretOnlyTemporaryConnection() {
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
    void testShowConnections() {
        tEnv.executeSql("CREATE TEMPORARY CONNECTION b_conn WITH ('k' = 'v')");
        tEnv.executeSql("CREATE TEMPORARY CONNECTION a_conn WITH ('k' = 'v')");

        assertThat(collectRows("SHOW CONNECTIONS"))
                .containsExactly(Row.of("a_conn"), Row.of("b_conn"));
    }

    @TestTemplate
    void testShowPermanentAndTemporaryConnections() throws Exception {
        ObjectIdentifier identifier = connectionIdentifier("permanent_conn");
        catalogManager()
                .getCatalog(identifier.getCatalogName())
                .orElseThrow()
                .createConnection(
                        identifier.toObjectPath(),
                        CatalogConnection.of(Map.of("k", "v"), null),
                        false);
        tEnv.executeSql("CREATE TEMPORARY CONNECTION temporary_conn WITH ('k' = 'v')");

        assertThat(collectRows("SHOW CONNECTIONS"))
                .containsExactly(Row.of("permanent_conn"), Row.of("temporary_conn"));
        assertThat(collectRows("SHOW CONNECTIONS FROM " + identifier.getDatabaseName()))
                .containsExactly(Row.of("permanent_conn"), Row.of("temporary_conn"));
    }

    @TestTemplate
    void testShowConnectionsLike() {
        tEnv.executeSql("CREATE TEMPORARY CONNECTION prod_conn WITH ('k' = 'v')");
        tEnv.executeSql("CREATE TEMPORARY CONNECTION tmp_conn WITH ('k' = 'v')");

        assertThat(collectRows("SHOW CONNECTIONS LIKE 'prod_%'"))
                .containsExactly(Row.of("prod_conn"));
        assertThat(collectRows("SHOW CONNECTIONS NOT LIKE 'prod_%'"))
                .containsExactly(Row.of("tmp_conn"));
    }

    @TestTemplate
    void testDescribeTemporaryConnection() {
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
    void testDescribeSecretOnlyConnectionIncludesDefaultType() {
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

    private List<Row> collectRows(String sql) {
        TableResult result = tEnv.executeSql(sql);
        return CollectionUtil.iteratorToList(result.collect());
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
