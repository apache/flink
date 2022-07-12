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

import org.apache.flink.table.factories.FactoryUtil;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link GenericInMemoryCatalog} created by {@link GenericInMemoryCatalogFactory}. */
class GenericInMemoryCatalogFactoryTest {

    @Test
    void test() throws Exception {
        final String catalogName = "mycatalog";
        final String databaseName = "mydatabase";

        final GenericInMemoryCatalog expectedCatalog =
                new GenericInMemoryCatalog(catalogName, databaseName);

        final Map<String, String> options = new HashMap<>();
        options.put(
                CommonCatalogOptions.CATALOG_TYPE.key(),
                GenericInMemoryCatalogFactoryOptions.IDENTIFIER);
        options.put(GenericInMemoryCatalogFactoryOptions.DEFAULT_DATABASE.key(), databaseName);

        final Catalog actualCatalog =
                FactoryUtil.createCatalog(
                        catalogName, options, null, Thread.currentThread().getContextClassLoader());

        checkEquals(expectedCatalog, (GenericInMemoryCatalog) actualCatalog);
    }

    private static void checkEquals(GenericInMemoryCatalog c1, GenericInMemoryCatalog c2)
            throws Exception {
        // Only assert a few selected properties for now
        assertThat(c2.getName()).isEqualTo(c1.getName());
        assertThat(c2.getDefaultDatabase()).isEqualTo(c1.getDefaultDatabase());
        assertThat(c2.listDatabases()).isEqualTo(c1.listDatabases());

        final String database = c1.getDefaultDatabase();

        assertThat(c2.listTables(database)).isEqualTo(c1.listTables(database));
        assertThat(c2.listViews(database)).isEqualTo(c1.listViews(database));
        assertThat(c2.listFunctions(database)).isEqualTo(c1.listFunctions(database));
    }
}
