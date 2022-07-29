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

package org.apache.flink.table.api.bridge.java.internal;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.table.utils.ExecutorMock;
import org.apache.flink.table.utils.PlannerMock;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StreamTableEnvironmentImpl}. */
class StreamTableEnvironmentImplTest {
    @Test
    void testAppendStreamDoesNotOverwriteTableConfig() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> elements = env.fromElements(1, 2, 3);

        StreamTableEnvironmentImpl tEnv = getStreamTableEnvironment(env, elements);

        Duration minRetention = Duration.ofMinutes(1);
        tEnv.getConfig().setIdleStateRetention(minRetention);
        Table table = tEnv.fromDataStream(elements);
        tEnv.toAppendStream(table, Row.class);

        assertThat(tEnv.getConfig().getIdleStateRetention()).isEqualTo(minRetention);
    }

    @Test
    void testRetractStreamDoesNotOverwriteTableConfig() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> elements = env.fromElements(1, 2, 3);

        StreamTableEnvironmentImpl tEnv = getStreamTableEnvironment(env, elements);

        Duration minRetention = Duration.ofMinutes(1);
        tEnv.getConfig().setIdleStateRetention(minRetention);
        Table table = tEnv.fromDataStream(elements);
        tEnv.toRetractStream(table, Row.class);

        assertThat(tEnv.getConfig().getIdleStateRetention()).isEqualTo(minRetention);
    }

    private StreamTableEnvironmentImpl getStreamTableEnvironment(
            StreamExecutionEnvironment env, DataStreamSource<Integer> elements) {
        TableConfig tableConfig = TableConfig.getDefault();
        CatalogManager catalogManager = CatalogManagerMocks.createEmptyCatalogManager();
        ModuleManager moduleManager = new ModuleManager();
        ResourceManager resourceManager =
                ResourceManager.createResourceManager(
                        new URL[0],
                        Thread.currentThread().getContextClassLoader(),
                        tableConfig.getConfiguration());

        return new StreamTableEnvironmentImpl(
                catalogManager,
                moduleManager,
                resourceManager,
                new FunctionCatalog(tableConfig, resourceManager, catalogManager, moduleManager),
                tableConfig,
                env,
                new TestPlanner(elements.getTransformation()),
                new ExecutorMock(),
                true);
    }

    private static class TestPlanner extends PlannerMock {
        private final Transformation<?> transformation;

        private TestPlanner(Transformation<?> transformation) {
            this.transformation = transformation;
        }

        @Override
        public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
            return Collections.singletonList(transformation);
        }
    }
}
