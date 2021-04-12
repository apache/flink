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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.utils.StreamTableTestUtil;
import org.apache.flink.util.Preconditions;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import scala.Some;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.BUILTIN_CATALOG_NAME;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.database;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.root;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.table;
import static org.apache.flink.table.catalog.PathResolutionTest.TestSpec.testSpec;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for path resolution in Table API & SQL. */
@RunWith(Parameterized.class)
public class PathResolutionTest {
    @Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() throws Exception {
        return asList(
                testSpec("simpleInDefaultPath")
                        .withCatalogManager(simpleCatalog())
                        .tableApiLookupPath("tab1")
                        .sqlLookupPath("tab1")
                        .expectPath(BUILTIN_CATALOG_NAME, "default", "tab1"),
                testSpec("simpleInChangedDefaultCatalog")
                        .withCatalogManager(simpleCatalog())
                        .withDefaultPath("cat1")
                        .tableApiLookupPath("tab1")
                        .sqlLookupPath("tab1")
                        .expectPath("cat1", "db1", "tab1"),
                testSpec("simpleInChangedDefaultPath")
                        .withCatalogManager(simpleCatalog())
                        .withDefaultPath("cat1", "db2")
                        .tableApiLookupPath("tab1")
                        .sqlLookupPath("tab1")
                        .expectPath("cat1", "db2", "tab1"),
                testSpec("qualifiedWithDatabase")
                        .withCatalogManager(simpleCatalog())
                        .withDefaultPath(BUILTIN_CATALOG_NAME, "default")
                        .tableApiLookupPath("db1", "tab1")
                        .sqlLookupPath("db1.tab1")
                        .expectPath(BUILTIN_CATALOG_NAME, "db1", "tab1"),
                testSpec("fullyQualifiedName")
                        .withCatalogManager(simpleCatalog())
                        .withDefaultPath(BUILTIN_CATALOG_NAME, "default")
                        .tableApiLookupPath("cat1", "db1", "tab1")
                        .sqlLookupPath("cat1.db1.tab1")
                        .expectPath("cat1", "db1", "tab1"),
                testSpec("dotInUnqualifiedTableName")
                        .withCatalogManager(catalogWithSpecialCharacters())
                        .tableApiLookupPath("tab.1")
                        .sqlLookupPath("`tab.1`")
                        .expectPath(BUILTIN_CATALOG_NAME, "default", "tab.1"),
                testSpec("dotInDatabaseName")
                        .withCatalogManager(catalogWithSpecialCharacters())
                        .tableApiLookupPath("default.db", "tab1")
                        .sqlLookupPath("`default.db`.tab1")
                        .expectPath(BUILTIN_CATALOG_NAME, "default.db", "tab1"),
                testSpec("dotInDefaultDatabaseName")
                        .withCatalogManager(catalogWithSpecialCharacters())
                        .withDefaultPath(BUILTIN_CATALOG_NAME, "default.db")
                        .tableApiLookupPath("tab1")
                        .sqlLookupPath("tab1")
                        .expectPath(BUILTIN_CATALOG_NAME, "default.db", "tab1"),
                testSpec("spaceInNames")
                        .withCatalogManager(catalogWithSpecialCharacters())
                        .tableApiLookupPath("default db", "tab 1")
                        .sqlLookupPath("`default db`.`tab 1`")
                        .expectPath(BUILTIN_CATALOG_NAME, "default db", "tab 1"),
                testSpec("shadowingWithTemporaryTable")
                        .withCatalogManager(catalogWithTemporaryObjects())
                        .tableApiLookupPath("cat1", "db1", "tab1")
                        .sqlLookupPath("cat1.db1.tab1")
                        .expectTemporaryPath("cat1", "db1", "tab1"));
    }

    private static CatalogManager simpleCatalog() throws Exception {
        return root().builtin(database("default", table("tab1")), database("db1", table("tab1")))
                .catalog("cat1", database("db1", table("tab1")), database("db2", table("tab1")))
                .build();
    }

    private static CatalogManager catalogWithTemporaryObjects() throws Exception {
        return root().builtin(database("default"))
                .catalog("cat1", database("db1", table("tab1")))
                .temporaryTable(ObjectIdentifier.of("cat1", "db1", "tab1"))
                .build();
    }

    private static CatalogManager catalogWithSpecialCharacters() throws Exception {
        return root().builtin(
                        database("default", table("tab.1")),
                        database("default.db", table("tab1"), table("tab.1")),
                        database("default db", table("tab 1")))
                .build();
    }

    @Parameter public TestSpec testSpec;

    @Test
    public void testTableApiPathResolution() {
        List<String> lookupPath = testSpec.getTableApiLookupPath();
        CatalogManager catalogManager = testSpec.getCatalogManager();
        testSpec.getDefaultCatalog().ifPresent(catalogManager::setCurrentCatalog);
        testSpec.getDefaultDatabase().ifPresent(catalogManager::setCurrentDatabase);

        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(lookupPath);
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        assertThat(
                Arrays.asList(
                        identifier.getCatalogName(),
                        identifier.getDatabaseName(),
                        identifier.getObjectName()),
                CoreMatchers.equalTo(testSpec.getExpectedPath()));
        Optional<CatalogManager.TableLookupResult> tableLookup =
                catalogManager.getTable(identifier);
        assertThat(tableLookup.isPresent(), is(true));
        assertThat(tableLookup.get().isTemporary(), is(testSpec.isTemporaryObject()));
    }

    @Test
    public void testStreamSqlPathResolution() {
        StreamTableTestUtil util =
                new StreamTableTestUtil(new Some<>(testSpec.getCatalogManager()));
        StreamTableEnvironmentImpl tEnv = util.javaTableEnv();

        testSpec.getDefaultCatalog().ifPresent(tEnv::useCatalog);
        testSpec.getDefaultDatabase().ifPresent(tEnv::useDatabase);

        util.verifyJavaSql(
                format("SELECT * FROM %s", testSpec.getSqlPathToLookup()),
                format(
                        "StreamTableSourceScan(table=[[%s]], fields=[], source=[isTemporary=[%s]])",
                        String.join(", ", testSpec.getExpectedPath()),
                        testSpec.isTemporaryObject()));
    }

    static class TestSpec {

        private String label;
        private String sqlPathToLookup;
        private List<String> tableApiLookupPath;
        private List<String> expectedPath;
        private boolean isTemporaryObject = false;
        private String defaultCatalog;
        private String defaultDatabase;
        private CatalogManager catalogManager;

        public TestSpec(String label) {
            this.label = label;
        }

        public static TestSpec testSpec(String label) {
            return new TestSpec(label);
        }

        public TestSpec withCatalogManager(CatalogManager catalogManager) {
            this.catalogManager = catalogManager;
            return this;
        }

        public TestSpec tableApiLookupPath(String... path) {
            this.tableApiLookupPath = asList(path);
            return this;
        }

        public TestSpec sqlLookupPath(String path) {
            this.sqlPathToLookup = path;
            return this;
        }

        public TestSpec expectPath(String... expectedPath) {
            Preconditions.checkArgument(
                    sqlPathToLookup != null && tableApiLookupPath != null,
                    "Both sql & table API versions of path lookups required. Remember expectPath needs to be called last");

            Preconditions.checkArgument(
                    catalogManager != null,
                    "A catalog manager needs to provided. Remember expectPath needs to be called last");

            this.expectedPath = asList(expectedPath);
            return this;
        }

        public TestSpec expectTemporaryPath(String... expectedPath) {
            this.isTemporaryObject = true;
            return expectPath(expectedPath);
        }

        public TestSpec withDefaultPath(String defaultCatalog) {
            this.defaultCatalog = defaultCatalog;
            return this;
        }

        public TestSpec withDefaultPath(String defaultCatalog, String defaultDatabase) {
            this.defaultCatalog = defaultCatalog;
            this.defaultDatabase = defaultDatabase;
            return this;
        }

        public String getSqlPathToLookup() {
            return sqlPathToLookup;
        }

        public List<String> getTableApiLookupPath() {
            return tableApiLookupPath;
        }

        public CatalogManager getCatalogManager() {
            return catalogManager;
        }

        public List<String> getExpectedPath() {
            return expectedPath;
        }

        public Optional<String> getDefaultCatalog() {
            return Optional.ofNullable(defaultCatalog);
        }

        public Optional<String> getDefaultDatabase() {
            return Optional.ofNullable(defaultDatabase);
        }

        public boolean isTemporaryObject() {
            return isTemporaryObject;
        }

        @Override
        public String toString() {

            StringBuilder stringBuilder = new StringBuilder();
            List<String> properties = new ArrayList<>();

            if (defaultCatalog != null) {
                properties.add("defaultCatalog: " + defaultCatalog);
            }

            if (defaultDatabase != null) {
                properties.add("defaultDatabase: " + defaultDatabase);
            }

            if (isTemporaryObject) {
                properties.add("temporary: true");
            }

            properties.add("sqlPath: " + sqlPathToLookup);
            properties.add("tableApiPath: " + tableApiLookupPath);
            properties.add("expectedPath: " + expectedPath);

            stringBuilder.append(format("%s=[%s]", label, String.join(", ", properties)));

            return stringBuilder.toString();
        }
    }
}
