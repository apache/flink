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

package org.apache.flink.table.planner.loader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.flink.table.planner.loader.PlannerModule.FLINK_TABLE_PLANNER_FAT_JAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the services loaded through {@link PlannerModule}.
 *
 * <p>This must be an ITCase so that it runs after the 'package' phase of maven. Otherwise, the
 * flink-table-planner jar will not be available.
 *
 * <p>This test might fail in your IDE if the flink-table-planner-loader module is wrongly configure
 * to include flink-table-planner in the test classpath.
 */
public class LoaderITCase extends TestLogger {

    @Test
    public void testExecutorFactory() {
        assertThat(
                        DelegateExecutorFactory.class
                                .getClassLoader()
                                .getResourceAsStream(FLINK_TABLE_PLANNER_FAT_JAR))
                .isNotNull();

        ExecutorFactory executorFactory =
                FactoryUtil.discoverFactory(
                        LoaderITCase.class.getClassLoader(),
                        ExecutorFactory.class,
                        ExecutorFactory.DEFAULT_IDENTIFIER);

        assertThat(executorFactory).isNotNull().isInstanceOf(DelegateExecutorFactory.class);
        assertThat(executorFactory.factoryIdentifier())
                .isEqualTo(ExecutorFactory.DEFAULT_IDENTIFIER);
    }

    @Test
    public void testPlannerFactory() {
        assertThat(
                        DelegatePlannerFactory.class
                                .getClassLoader()
                                .getResourceAsStream(FLINK_TABLE_PLANNER_FAT_JAR))
                .isNotNull();

        PlannerFactory plannerFactory =
                FactoryUtil.discoverFactory(
                        LoaderITCase.class.getClassLoader(),
                        PlannerFactory.class,
                        PlannerFactory.DEFAULT_IDENTIFIER);

        assertThat(plannerFactory).isNotNull().isInstanceOf(DelegatePlannerFactory.class);
        assertThat(plannerFactory.factoryIdentifier()).isEqualTo(PlannerFactory.DEFAULT_IDENTIFIER);
    }

    @Test
    public void testPlannerJarLeak() throws IOException {
        PlannerModule plannerModule = PlannerModule.getInstance();
        final Path tmpDirectory =
                Paths.get(ConfigurationUtils.parseTempDirectories(new Configuration())[0]);
        Files.createDirectories(FileUtils.getTargetPathIfContainsSymbolicPath(tmpDirectory));
        assertThat(tmpDirectory.startsWith("flink-table-planner_")).isEqualTo(false);
    }
}
