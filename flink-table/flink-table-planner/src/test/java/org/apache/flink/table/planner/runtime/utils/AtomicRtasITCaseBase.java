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

package org.apache.flink.table.planner.runtime.utils;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.connector.sink.abilities.SupportsStaging;
import org.apache.flink.table.planner.factories.TestSupportsStagingTableFactory;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The base case of atomic rtas ITCase. */
public abstract class AtomicRtasITCaseBase extends TestLogger {

    protected TableEnvironment tEnv;

    protected abstract TableEnvironment getTableEnvironment();

    @BeforeEach
    void setup() {
        tEnv = getTableEnvironment();
        List<Row> sourceData = Collections.singletonList(Row.of(1, "ZM"));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        String sourceDDL = "create table t1(a int, b varchar) with ('connector' = 'COLLECTION')";
        tEnv.executeSql(sourceDDL);
    }

    @AfterEach
    void clean() {
        // clean data
        TestSupportsStagingTableFactory.JOB_STATUS_CHANGE_PROCESS.clear();
        TestSupportsStagingTableFactory.STAGING_PURPOSE_LIST.clear();
    }

    @Test
    void testAtomicReplaceTableAs(@TempDir Path temporaryFolder) throws Exception {
        commonTestForAtomicReplaceTableAs(
                "atomic_replace_table", false, true, temporaryFolder.toFile());
    }

    @Test
    void testAtomicReplaceTableAsWithReplacedTableNotExists(@TempDir Path temporaryFolder)
            throws Exception {
        commonTestForAtomicReplaceTableAs(
                "atomic_replace_table_not_exists", false, false, temporaryFolder.toFile());
    }

    @Test
    void testAtomicCreateOrReplaceTableAs(@TempDir Path temporaryFolder) throws Exception {
        commonTestForAtomicReplaceTableAs(
                "atomic_create_or_replace_table", true, true, temporaryFolder.toFile());
    }

    @Test
    void testAtomicCreateOrReplaceTableAsWithReplacedTableNotExists(@TempDir Path temporaryFolder)
            throws Exception {
        commonTestForAtomicReplaceTableAs(
                "atomic_create_or_replace_table_not_exists", true, false, temporaryFolder.toFile());
    }

    private void commonTestForAtomicReplaceTableAs(
            String tableName,
            boolean isCreateOrReplace,
            boolean isCreateReplacedTable,
            File tmpDataFolder)
            throws Exception {
        if (isCreateReplacedTable) {
            tEnv.executeSql("create table " + tableName + " (a int) with ('connector' = 'PRINT')");
        }

        tEnv.getConfig().set(TableConfigOptions.TABLE_RTAS_CTAS_ATOMICITY_ENABLED, true);
        String dataDir = tmpDataFolder.getAbsolutePath();
        String sqlFragment = getCreateOrReplaceSqlFragment(isCreateOrReplace, tableName);
        String sql =
                sqlFragment
                        + " with ('connector' = 'test-staging', 'data-dir' = '"
                        + dataDir
                        + "') as select * from t1";
        if (!isCreateOrReplace && !isCreateReplacedTable) {
            assertThatThrownBy(() -> tEnv.executeSql(sql))
                    .isInstanceOf(TableException.class)
                    .hasMessage(
                            "The table `default_catalog`.`default_database`.`"
                                    + tableName
                                    + "` to be replaced doesn't exist."
                                    + " You can try to use CREATE TABLE AS statement or CREATE OR REPLACE TABLE AS statement.");
        } else {
            tEnv.executeSql(sql).await();
            if (isCreateReplacedTable) {
                assertThat(tEnv.listTables()).contains(tableName);
            } else {
                assertThat(tEnv.listTables()).doesNotContain(tableName);
            }
            verifyDataFile(dataDir, "data");
            assertThat(TestSupportsStagingTableFactory.JOB_STATUS_CHANGE_PROCESS).hasSize(2);
            assertThat(TestSupportsStagingTableFactory.JOB_STATUS_CHANGE_PROCESS)
                    .contains("begin", "commit");
            assertThat(TestSupportsStagingTableFactory.STAGING_PURPOSE_LIST).hasSize(1);
            if (isCreateOrReplace) {
                assertThat(TestSupportsStagingTableFactory.STAGING_PURPOSE_LIST)
                        .contains(SupportsStaging.StagingPurpose.CREATE_OR_REPLACE_TABLE_AS);
            } else {
                assertThat(TestSupportsStagingTableFactory.STAGING_PURPOSE_LIST)
                        .contains(SupportsStaging.StagingPurpose.REPLACE_TABLE_AS);
            }
        }
    }

    @Test
    void testAtomicReplaceTableAsWithException(@TempDir Path temporaryFolder) {
        commonTestForAtomicReplaceTableAsWithException(
                "atomic_replace_table_fail", false, temporaryFolder.toFile());
    }

    @Test
    void testAtomicCreateOrReplaceTableAsWithException(@TempDir Path temporaryFolder) {
        commonTestForAtomicReplaceTableAsWithException(
                "atomic_create_or_replace_table_fail", true, temporaryFolder.toFile());
    }

    private void commonTestForAtomicReplaceTableAsWithException(
            String tableName, boolean isCreateOrReplace, File tmpDataFolder) {
        tEnv.executeSql("create table " + tableName + " (a int) with ('connector' = 'PRINT')");
        tEnv.getConfig().set(TableConfigOptions.TABLE_RTAS_CTAS_ATOMICITY_ENABLED, true);
        String dataDir = tmpDataFolder.getAbsolutePath();
        String sqlFragment = getCreateOrReplaceSqlFragment(isCreateOrReplace, tableName);
        assertThatCode(
                        () ->
                                tEnv.executeSql(
                                                sqlFragment
                                                        + " with ('connector' = 'test-staging', 'data-dir' = '"
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
    void testWithoutAtomicReplaceTableAs(@TempDir Path temporaryFolder) throws Exception {
        commonTestForWithoutAtomicReplaceTableAs(
                "non_atomic_replace_table", false, temporaryFolder.toFile());
    }

    @Test
    void testWithoutAtomicCreateOrReplaceTableAs(@TempDir Path temporaryFolder) throws Exception {
        commonTestForWithoutAtomicReplaceTableAs(
                "non_atomic_create_or_replace_table", true, temporaryFolder.toFile());
    }

    private void commonTestForWithoutAtomicReplaceTableAs(
            String tableName, boolean isCreateOrReplace, File tmpDataFolder) throws Exception {
        tEnv.executeSql("create table " + tableName + " (a int) with ('connector' = 'PRINT')");
        tEnv.getConfig().set(TableConfigOptions.TABLE_RTAS_CTAS_ATOMICITY_ENABLED, false);
        String dataDir = tmpDataFolder.getAbsolutePath();
        String sqlFragment = getCreateOrReplaceSqlFragment(isCreateOrReplace, tableName);

        tEnv.executeSql(
                        sqlFragment
                                + " with ('connector' = 'test-staging', 'data-dir' = '"
                                + dataDir
                                + "') as select * from t1")
                .await();
        assertThat(tEnv.listTables()).contains(tableName);
        // Not using StagedTable, so need to read the hidden file
        verifyDataFile(dataDir, "_data");
        assertThat(TestSupportsStagingTableFactory.JOB_STATUS_CHANGE_PROCESS).hasSize(0);
        assertThat(TestSupportsStagingTableFactory.STAGING_PURPOSE_LIST).hasSize(0);
    }

    private void verifyDataFile(String dataDir, String fileName) throws IOException {
        File dataFile = new File(dataDir, fileName);
        assertThat(dataFile).exists();
        assertThat(dataFile).isFile();
        assertThat(FileUtils.readFileUtf8(dataFile)).isEqualTo("1,ZM");
    }

    private String getCreateOrReplaceSqlFragment(boolean isCreateOrReplace, String tableName) {
        return isCreateOrReplace
                ? " create or replace table " + tableName
                : " replace table " + tableName;
    }
}
