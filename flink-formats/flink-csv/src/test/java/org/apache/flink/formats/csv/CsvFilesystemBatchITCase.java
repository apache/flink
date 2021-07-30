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

package org.apache.flink.formats.csv;

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.runtime.batch.sql.BatchFileSystemITCaseBase;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FileUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** ITCase to test csv format for {@link CsvFileSystemFormatFactory} in batch mode. */
@RunWith(Enclosed.class)
public class CsvFilesystemBatchITCase {

    /** General IT cases for CsvRowDataFilesystem in batch mode. */
    public static class GeneralCsvFilesystemBatchITCase extends BatchFileSystemITCaseBase {

        @Override
        public String[] formatProperties() {
            List<String> ret = new ArrayList<>();
            ret.add("'format'='csv'");
            ret.add("'csv.field-delimiter'=';'");
            ret.add("'csv.quote-character'='#'");
            return ret.toArray(new String[0]);
        }
    }

    /**
     * Enriched IT cases that including testParseError and testEscapeChar for CsvRowDataFilesystem
     * in batch mode.
     */
    public static class EnrichedCsvFilesystemBatchITCase extends BatchFileSystemITCaseBase {

        @Override
        public String[] formatProperties() {
            List<String> ret = new ArrayList<>();
            ret.add("'format'='csv'");
            ret.add("'csv.ignore-parse-errors'='true'");
            ret.add("'csv.escape-character'='\t'");
            return ret.toArray(new String[0]);
        }

        @Test
        public void testParseError() throws Exception {
            String path = new URI(resultPath()).getPath();
            new File(path).mkdirs();
            File file = new File(path, "test_file");
            file.createNewFile();
            FileUtils.writeFileUtf8(file, "x5,5,1,1\n" + "x5,5,2,2,2\n" + "x5,5,1,1");

            check(
                    "select * from nonPartitionedTable",
                    Arrays.asList(Row.of("x5,5,1,1"), Row.of("x5,5,1,1")));
        }

        @Test
        public void testEscapeChar() throws Exception {
            String path = new URI(resultPath()).getPath();
            new File(path).mkdirs();
            File file = new File(path, "test_file");
            file.createNewFile();
            FileUtils.writeFileUtf8(file, "x5,\t\n5,1,1\n" + "x5,\t5,2,2");

            check(
                    "select * from nonPartitionedTable",
                    Arrays.asList(Row.of("x5,5,1,1"), Row.of("x5,5,2,2")));
        }
    }

    /**
     * IT case which checks for a bug in Jackson 2.10. When the 4000th character in csv file is the
     * new line character (\n) an exception will be thrown. After upgrading jackson to >= 2.11 this
     * bug should not exist anymore.
     */
    public static class JacksonVersionUpgradeITCase extends BatchTestBase {

        @Test
        public void testCsvFileWithNewLineAt4000() throws Exception {
            StringBuilder csvContent = new StringBuilder("# ");
            for (int i = 0; i < 97; i++) {
                csvContent.append("-");
            }
            csvContent.append("\n");
            for (int i = 0; i < 50; i++) {
                for (int j = 0; j < 49; j++) {
                    csvContent.append("a");
                }
                csvContent.append(",");
                for (int j = 0; j < 49; j++) {
                    csvContent.append("b");
                }
                csvContent.append("\n");
            }

            File tempCsvFile = File.createTempFile("new-line-at-4000", ".csv");
            tempCsvFile.createNewFile();
            FileUtils.writeFileUtf8(tempCsvFile, csvContent.toString());

            tEnv().getConfig()
                    .getConfiguration()
                    .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
            tEnv().executeSql(
                            "CREATE TABLE T (\n"
                                    + "  a VARCHAR,\n"
                                    + "  b VARCHAR\n"
                                    + ") WITH (\n"
                                    + "  'connector' = 'filesystem',\n"
                                    + "  'path' = 'file://"
                                    + tempCsvFile.toString()
                                    + "',\n"
                                    + "  'format' = 'csv',\n"
                                    + "  'csv.allow-comments' = 'true'\n"
                                    + ")")
                    .await();
            List<Row> results =
                    CollectionUtil.iteratorToList(
                            tEnv().executeSql("SELECT a, b FROM T").collect());

            Assert.assertEquals(50, results.size());
            for (Row actual : results) {
                StringBuilder a = new StringBuilder();
                for (int i = 0; i < 49; i++) {
                    a.append("a");
                }
                StringBuilder b = new StringBuilder();
                for (int i = 0; i < 49; i++) {
                    b.append("b");
                }
                Assert.assertEquals(2, actual.getArity());
                Assert.assertEquals(a.toString(), actual.getField(0));
                Assert.assertEquals(b.toString(), actual.getField(1));
            }
        }
    }
}
