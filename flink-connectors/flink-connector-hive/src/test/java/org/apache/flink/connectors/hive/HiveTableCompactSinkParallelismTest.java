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

package org.apache.flink.connectors.hive;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.connector.file.table.batch.BatchSink;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.COMPACTION_PARALLELISM;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests to verify operator's parallelism of {@link HiveTableSink} enabled auto-compaction. */
@ExtendWith(TestLoggerExtension.class)
class HiveTableCompactSinkParallelismTest {
    /**
     * Represents the parallelism doesn't need to be checked, it should follow the setting of planer
     * or auto inference.
     */
    public static final int NO_NEED_TO_CHECK_PARALLELISM = -1;

    private HiveCatalog catalog;

    private TableEnvironment tableEnv;

    @BeforeEach
    void before() {
        catalog = HiveTestUtils.createHiveCatalog();
        catalog.open();
        tableEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        tableEnv.registerCatalog(catalog.getName(), catalog);
        tableEnv.useCatalog(catalog.getName());
    }

    @AfterEach
    void after() {
        if (catalog != null) {
            catalog.close();
        }
    }

    /** If only sink parallelism is set, compact operator should follow this setting. */
    @Test
    void testOnlySetSinkParallelism() {
        final int sinkParallelism = 4;

        tableEnv.executeSql(
                String.format(
                        "CREATE TABLE src ("
                                + " key string,"
                                + " value string"
                                + ") TBLPROPERTIES ("
                                + " 'auto-compaction' = 'true', "
                                + " '%s' = '%s' )",
                        SINK_PARALLELISM.key(), sinkParallelism));

        assertSinkAndCompactOperatorParallelism(true, true, sinkParallelism, sinkParallelism);
    }

    @Test
    void testOnlySetCompactParallelism() {
        final int compactParallelism = 4;

        tableEnv.executeSql(
                String.format(
                        "CREATE TABLE src ("
                                + " key string,"
                                + " value string"
                                + ") TBLPROPERTIES ("
                                + " 'auto-compaction' = 'true', "
                                + " '%s' = '%s' )",
                        COMPACTION_PARALLELISM.key(), compactParallelism));

        assertSinkAndCompactOperatorParallelism(
                false, true, NO_NEED_TO_CHECK_PARALLELISM, compactParallelism);
    }

    @Test
    void testSetBothSinkAndCompactParallelism() {
        final int sinkParallelism = 8;
        final int compactParallelism = 4;

        tableEnv.executeSql(
                String.format(
                        "CREATE TABLE src ("
                                + " key string,"
                                + " value string"
                                + ") TBLPROPERTIES ("
                                + " 'auto-compaction' = 'true', "
                                + " '%s' = '%s', "
                                + " '%s' = '%s' )",
                        SINK_PARALLELISM.key(),
                        sinkParallelism,
                        COMPACTION_PARALLELISM.key(),
                        compactParallelism));

        assertSinkAndCompactOperatorParallelism(true, true, sinkParallelism, compactParallelism);
    }

    @Test
    void testSinkAndCompactAllNotSetParallelism() {
        tableEnv.executeSql(
                "CREATE TABLE src ("
                        + " key string,"
                        + " value string"
                        + ") TBLPROPERTIES ("
                        + " 'auto-compaction' = 'true' )");
        assertSinkAndCompactOperatorParallelism(
                false, false, NO_NEED_TO_CHECK_PARALLELISM, NO_NEED_TO_CHECK_PARALLELISM);
    }

    private void assertSinkAndCompactOperatorParallelism(
            boolean isSinkParallelismConfigured,
            boolean isCompactParallelismConfigured,
            int expectedSinkParallelism,
            int expectedCompactParallelism) {
        String statement = "insert into src values ('k1', 'v1'), ('k2', 'v2');";
        PlannerBase planner = (PlannerBase) ((TableEnvironmentImpl) tableEnv).getPlanner();
        planner.getExecEnv().setParallelism(10);
        List<Operation> operations = planner.getParser().parse(statement);
        List<Transformation<?>> transformations =
                planner.translate(Collections.singletonList((ModifyOperation) (operations.get(0))));
        assertThat(transformations).hasSize(1);
        Transformation<?> rootTransformation = transformations.get(0);
        Transformation<?> compactTransformation =
                findTransformationByName(rootTransformation, BatchSink.COMPACT_OP_NAME);
        Transformation<?> hiveSinkTransformation =
                findTransformationByName(
                        rootTransformation, HiveTableSink.BATCH_COMPACT_WRITER_OP_NAME);
        assertThat(hiveSinkTransformation.isParallelismConfigured())
                .isEqualTo(isSinkParallelismConfigured);
        assertThat(compactTransformation.isParallelismConfigured())
                .isEqualTo(isCompactParallelismConfigured);
        if (expectedSinkParallelism != NO_NEED_TO_CHECK_PARALLELISM) {
            assertThat(hiveSinkTransformation.getParallelism()).isEqualTo(expectedSinkParallelism);
        }
        if (expectedCompactParallelism != NO_NEED_TO_CHECK_PARALLELISM) {
            assertThat(compactTransformation.getParallelism())
                    .isEqualTo(expectedCompactParallelism);
        }
    }

    /**
     * This method will recursively look forward to the transformation whose name meets the
     * requirements. For simplicity, let's assume that each transformation has at most one input.
     */
    private static Transformation<?> findTransformationByName(
            Transformation<?> transformation, String name) {
        if (transformation == null) {
            return null;
        }

        if (transformation.getName().equals(name)) {
            return transformation;
        }

        return findTransformationByName(transformation.getInputs().get(0), name);
    }
}
