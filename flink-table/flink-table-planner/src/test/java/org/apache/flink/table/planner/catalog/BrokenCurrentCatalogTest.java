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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a fully-qualified query against a healthy catalog can still be resolved even when the
 * current catalog is unreachable.
 */
class BrokenCurrentCatalogTest {

    @Test
    void testFullyQualifiedQueryWhileCurrentCatalogIsBroken() throws Exception {
        TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.registerCatalog("healthy", new GenericInMemoryCatalog("healthy", "default"));
        tEnv.useCatalog("healthy");
        tEnv.executeSql(
                "CREATE VIEW `healthy`.`default`.`v` AS SELECT * FROM (VALUES (1), (2), (3)) AS t(id)");

        tEnv.registerCatalog("broken", new UnreachableCatalog("broken"));
        tEnv.useCatalog("broken");

        Table table = tEnv.sqlQuery("SELECT * FROM `healthy`.`default`.`v`");

        assertThat(table.getResolvedSchema().getColumnNames()).containsExactly("id");
    }

    private static class UnreachableCatalog extends GenericInMemoryCatalog {
        UnreachableCatalog(String name) {
            super(name, "default");
        }

        @Override
        public boolean databaseExists(String databaseName) {
            throw new CatalogException(
                    "Failed to connect to database '"
                            + databaseName
                            + "' of catalog '"
                            + getName()
                            + "'.");
        }
    }
}
