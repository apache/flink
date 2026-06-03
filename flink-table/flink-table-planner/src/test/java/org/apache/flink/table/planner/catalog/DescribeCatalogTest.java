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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStore;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@code DESCRIBE CATALOG} SQL statement, focusing on sensitive-option redaction. */
class DescribeCatalogTest {

    private static final String CATALOG_NAME = "test_cat";

    @Test
    void describeCatalogExtendedRedactsSensitiveOptions() throws Exception {
        TableEnvironment tEnv = buildEnvWithSensitiveOptions();

        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("DESCRIBE CATALOG EXTENDED " + CATALOG_NAME).collect());

        assertThat(rows)
                .contains(
                        Row.of("option:password", GlobalConfiguration.HIDDEN_CONTENT),
                        Row.of("option:my.token", GlobalConfiguration.HIDDEN_CONTENT),
                        Row.of("option:safe-option", "safe-value"))
                .noneMatch(r -> "topsecret".equals(r.getField(1)))
                .noneMatch(r -> "tok123".equals(r.getField(1)));
    }

    @Test
    void describeCatalogNonExtendedDoesNotExposeOptions() throws Exception {
        TableEnvironment tEnv = buildEnvWithSensitiveOptions();

        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("DESCRIBE CATALOG " + CATALOG_NAME).collect());

        assertThat(rows).noneMatch(r -> String.valueOf(r.getField(0)).startsWith("option:"));
    }

    private static TableEnvironment buildEnvWithSensitiveOptions() throws Exception {
        GenericInMemoryCatalogStore catalogStore = new GenericInMemoryCatalogStore();
        catalogStore.open();
        Configuration config = new Configuration();
        config.setString("type", "generic_in_memory");
        config.setString("safe-option", "safe-value");
        config.setString("password", "topsecret");
        config.setString("my.token", "tok123");
        catalogStore.storeCatalog(CATALOG_NAME, CatalogDescriptor.of(CATALOG_NAME, config));
        return TableEnvironment.create(
                EnvironmentSettings.newInstance().withCatalogStore(catalogStore).build());
    }
}
