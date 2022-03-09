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
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/** Tests for {@link TableEnvironment} that require a planner. */
public class EnvironmentTest {

    @Test
    public void testPassingExecutionParameters() {
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

        assertEquals(128, env.getParallelism());
        assertEquals(800, env.getConfig().getAutoWatermarkInterval());
        assertEquals(30000, env.getCheckpointConfig().getCheckpointInterval());
    }

    @Test
    public void testEnvironmentSettings() throws ExecutionException, InterruptedException {
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
}
