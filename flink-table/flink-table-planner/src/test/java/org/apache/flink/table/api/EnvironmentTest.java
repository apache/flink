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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStore;
import org.apache.flink.table.catalog.listener.CatalogListener1;
import org.apache.flink.table.catalog.listener.CatalogListener2;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link TableEnvironment} that require a planner. */
class EnvironmentTest {

    @Test
    void testPassingExecutionParameters() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.getConfig()
                .addConfiguration(
                        new Configuration()
                                .set(CoreOptions.DEFAULT_PARALLELISM, 128)
                                .set(
                                        PipelineOptions.AUTO_WATERMARK_INTERVAL,
                                        Duration.ofMillis(800))
                                .set(
                                        ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                                        Duration.ofSeconds(30)));

        tEnv.createTemporaryView("test", env.fromElements(1, 2, 3));

        // trigger translation
        Table table = tEnv.sqlQuery("SELECT * FROM test");
        tEnv.toAppendStream(table, Row.class);

        assertThat(env.getParallelism()).isEqualTo(128);
        assertThat(env.getConfig().getAutoWatermarkInterval()).isEqualTo(800);
        assertThat(env.getCheckpointConfig().getCheckpointInterval()).isEqualTo(30000);
    }

    @Test
    void testEnvironmentSettings() throws ExecutionException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set(TableConfigOptions.TABLE_CATALOG_NAME, "myCatalog");
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().withConfiguration(conf).build();

        TableEnvironment tEnv = TableEnvironment.create(settings);
        assertThat(tEnv.getConfig().get(TableConfigOptions.TABLE_CATALOG_NAME))
                .isEqualTo("myCatalog");
        assertThat(tEnv.getCurrentCatalog()).isEqualTo("myCatalog");

        StreamTableEnvironment stEnv =
                StreamTableEnvironment.create(
                        StreamExecutionEnvironment.getExecutionEnvironment(), settings);
        assertThat(stEnv.getConfig().get(TableConfigOptions.TABLE_CATALOG_NAME))
                .isEqualTo("myCatalog");

        stEnv.getConfig()
                .set(
                        TableConfigOptions.TABLE_CATALOG_NAME,
                        TableConfigOptions.TABLE_CATALOG_NAME.defaultValue());
        assertThat(stEnv.getCurrentCatalog()).isEqualTo("myCatalog");
    }

    @Test
    void testCreateCatalogModificationListenersForTable() {
        Configuration configuration = new Configuration();
        TableEnvironmentImpl env1 =
                (TableEnvironmentImpl)
                        TableEnvironment.create(
                                EnvironmentSettings.newInstance()
                                        .withConfiguration(configuration)
                                        .build());
        assertThat(env1.getCatalogManager().getCatalogModificationListeners().isEmpty()).isTrue();

        configuration.setString(
                TableConfigOptions.TABLE_CATALOG_MODIFICATION_LISTENERS.key(), "factory1;factory2");
        TableEnvironmentImpl env2 =
                (TableEnvironmentImpl)
                        TableEnvironment.create(
                                EnvironmentSettings.newInstance()
                                        .withConfiguration(configuration)
                                        .build());
        assertThat(
                        Arrays.asList(
                                CatalogListener1.class.getName(), CatalogListener2.class.getName()))
                .isEqualTo(
                        env2.getCatalogManager().getCatalogModificationListeners().stream()
                                .map(c -> c.getClass().getName())
                                .collect(Collectors.toList()));
    }

    @Test
    void testCreateCatalogModificationListenersForStreamTable() {
        Configuration configuration = new Configuration();
        StreamTableEnvironmentImpl env1 =
                (StreamTableEnvironmentImpl)
                        StreamTableEnvironment.create(
                                StreamExecutionEnvironment.getExecutionEnvironment(),
                                EnvironmentSettings.newInstance()
                                        .withConfiguration(configuration)
                                        .build());
        assertThat(env1.getCatalogManager().getCatalogModificationListeners().isEmpty()).isTrue();

        configuration.setString(
                TableConfigOptions.TABLE_CATALOG_MODIFICATION_LISTENERS.key(), "factory1;factory2");
        StreamTableEnvironmentImpl env2 =
                (StreamTableEnvironmentImpl)
                        StreamTableEnvironment.create(
                                StreamExecutionEnvironment.getExecutionEnvironment(),
                                EnvironmentSettings.newInstance()
                                        .withConfiguration(configuration)
                                        .build());
        assertThat(
                        Arrays.asList(
                                CatalogListener1.class.getName(), CatalogListener2.class.getName()))
                .isEqualTo(
                        env2.getCatalogManager().getCatalogModificationListeners().stream()
                                .map(c -> c.getClass().getName())
                                .collect(Collectors.toList()));
    }

    @Test
    void testRegisterCatalogStoreUsingTableApi() {
        CatalogStore catalogStore = new GenericInMemoryCatalogStore();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().withCatalogStore(catalogStore).build();

        TableEnvironment tbEnv = TableEnvironment.create(settings);

        Configuration configuration = new Configuration();
        configuration.setString("type", "generic_in_memory");
        tbEnv.createCatalog("test_catalog", CatalogDescriptor.of("test_catalog", configuration));

        assertTrue(catalogStore.contains("test_catalog"));
    }
}
