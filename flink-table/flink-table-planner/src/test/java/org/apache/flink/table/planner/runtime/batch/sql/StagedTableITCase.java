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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.planner.factories.TestSupportsStagingTableFactory;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests staged table in batch mode. */
public class StagedTableITCase extends BatchTestBase {

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    @Override
    public void before() throws Exception {
        super.before();
        List<Row> sourceData = Arrays.asList(Row.of(1, "ZM"));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        String sourceDDL = "create table t1(a int, b varchar) with ('connector' = 'COLLECTION')";
        tEnv().executeSql(sourceDDL);
    }

    @Test
    public void testStagedTableWithAtomicCtas() throws Exception {
        tableConfig().set(TableConfigOptions.TABLE_CTAS_ATOMICITY_ENABLED, true);
        String dataDir = tempFolder.newFolder().getAbsolutePath();
        tEnv().executeSql(
                        "create table ctas_batch_table with ('connector' = 'test-staging', 'data-dir' = '"
                                + dataDir
                                + "') as select * from t1")
                .await();
        File file = new File(dataDir, "data");
        assertThat(file).exists();
        assertThat(file).isFile();
        assertThat(FileUtils.readFileUtf8(file)).isEqualTo("1,ZM");
        assertThat(TestSupportsStagingTableFactory.JOB_STATUS_CHANGE_PROCESS).hasSize(2);
        assertThat(TestSupportsStagingTableFactory.JOB_STATUS_CHANGE_PROCESS)
                .contains("begin", "commit");
    }
}
