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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.connector.sink.abilities.SupportsStaging;
import org.apache.flink.table.planner.factories.TestSupportsStagingTableFactory;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests staged table in stream mode. */
public class StagedTableITCase extends StreamingTestBase {

    @TempDir Path temporaryFolder;

    private File tmpDataFolder;

    @BeforeEach
    void setup() throws Exception {
        super.before();
        List<Row> sourceData = Arrays.asList(Row.of(1, "ZM"));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        String sourceDDL = "create table t1(a int, b varchar) with ('connector' = 'COLLECTION')";
        tEnv().executeSql(sourceDDL);

        tmpDataFolder = TempDirUtils.newFolder(temporaryFolder);
    }

    @AfterEach
    void close() {
        if (tmpDataFolder != null) {
            tmpDataFolder.delete();
        }
    }

    @Test
    void testStagedTableWithAtomicCtas() throws Exception {
        commonTestStagedTableWithAtomicCtas("ctas_stream_table", false);
    }

    @Test
    void testStagedTableWithAtomicCtasIfNotExists() throws Exception {
        commonTestStagedTableWithAtomicCtas("ctas_if_not_exists_stream_table", true);
    }

    void commonTestStagedTableWithAtomicCtas(String tableName, boolean ifNotExists)
            throws Exception {
        tEnv().getConfig().set(TableConfigOptions.TABLE_CTAS_ATOMICITY_ENABLED, true);
        String dataDir = tmpDataFolder.getAbsolutePath();
        String sqlFragment = ifNotExists ? " if not exists " + tableName : tableName;
        tEnv().executeSql(
                        "create table "
                                + sqlFragment
                                + " with ('connector' = 'test-staging', 'data-dir' = '"
                                + dataDir
                                + "') as select * from t1")
                .await();
        assertThat(tEnv().listTables()).doesNotContain(tableName);
        File file = new File(dataDir, "data");
        assertThat(file).exists();
        assertThat(file).isFile();
        assertThat(FileUtils.readFileUtf8(file)).isEqualTo("1,ZM");
        assertThat(TestSupportsStagingTableFactory.JOB_STATUS_CHANGE_PROCESS).hasSize(2);
        assertThat(TestSupportsStagingTableFactory.JOB_STATUS_CHANGE_PROCESS)
                .contains("begin", "commit");
        assertThat(TestSupportsStagingTableFactory.STAGING_PURPOSE_LIST).hasSize(1);
        if (ifNotExists) {
            assertThat(TestSupportsStagingTableFactory.STAGING_PURPOSE_LIST)
                    .contains(SupportsStaging.StagingPurpose.CREATE_TABLE_AS_IF_NOT_EXISTS);
        } else {
            assertThat(TestSupportsStagingTableFactory.STAGING_PURPOSE_LIST)
                    .contains(SupportsStaging.StagingPurpose.CREATE_TABLE_AS);
        }
    }

    @Test
    void testFailStagedTableWithAtomicCtas() throws Exception {
        tEnv().getConfig().set(TableConfigOptions.TABLE_CTAS_ATOMICITY_ENABLED, true);
        String dataDir = tmpDataFolder.getAbsolutePath();
        assertThatCode(
                        () ->
                                tEnv().executeSql(
                                                "create table ctas_stream_table_fail with ('connector' = 'test-staging', 'data-dir' = '"
                                                        + dataDir
                                                        + "', 'sink-fail' = '"
                                                        + true
                                                        + "') as select * from t1")
                                        .await())
                .hasRootCauseMessage("Test StagedTable abort method.");

        assertThat(TestSupportsStagingTableFactory.JOB_STATUS_CHANGE_PROCESS).hasSize(2);
        assertThat(TestSupportsStagingTableFactory.JOB_STATUS_CHANGE_PROCESS)
                .contains("begin", "abort");
    }

    @Test
    void testStagedTableWithoutAtomicCtas() throws Exception {
        tEnv().getConfig().set(TableConfigOptions.TABLE_CTAS_ATOMICITY_ENABLED, false);
        String dataDir = tmpDataFolder.getAbsolutePath();
        tEnv().executeSql(
                        "create table ctas_stream_table with ('connector' = 'test-staging', 'data-dir' = '"
                                + dataDir
                                + "') as select * from t1")
                .await();
        assertThat(tEnv().listTables()).contains("ctas_stream_table");
        // Not using StagedTable, so need to read the hidden file
        File file = new File(dataDir, "_data");
        assertThat(file).exists();
        assertThat(file).isFile();
        assertThat(FileUtils.readFileUtf8(file)).isEqualTo("1,ZM");
        assertThat(TestSupportsStagingTableFactory.JOB_STATUS_CHANGE_PROCESS).hasSize(0);
        assertThat(TestSupportsStagingTableFactory.STAGING_PURPOSE_LIST).hasSize(0);
    }
}
