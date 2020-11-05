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

import org.apache.flink.table.planner.runtime.batch.sql.BatchFileSystemITCaseBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

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
                    Arrays.asList(Row.of("x5", 5, 1, 1), Row.of("x5", 5, 1, 1)));
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
                    Arrays.asList(Row.of("x5", 5, 1, 1), Row.of("x5", 5, 2, 2)));
        }

        @Test
        public void testFilenamePathMetadata() throws Exception {
            String path = new URI(resultPath()).getPath();
            new File(path).mkdirs();
            File file1 = new File(path, "test_file1");
            file1.createNewFile();
            FileUtils.writeFileUtf8(file1, "x8,8,1,1\n" + "x8,8,3,3");
            File file2 = new File(path, "test_file2");
            file2.createNewFile();
            FileUtils.writeFileUtf8(file2, "x8,8,2,2");

            check(
                    "select x,y,a,b,`flink.fs.path` from withMetadataTable",
                    Arrays.asList(
                            // TODO: should path have a leading file:/ or not?
                            Row.of("x8,8,1,1,file:" + file1.getPath()),
                            Row.of("x8,8,2,2,file:" + file2.getPath()),
                            Row.of("x8,8,3,3,file:" + file1.getPath())));

            check(
                    "select x,y,a,b,`flink.fs.path` from withMetadataTable2",
                    Arrays.asList(
                            Row.of("x8,8,1,1,file:" + file1.getPath()),
                            Row.of("x8,8,2,2,file:" + file2.getPath()),
                            Row.of("x8,8,3,3,file:" + file1.getPath())));

            check(
                    "select x,y,a,b,`flink.fs.path` from withMetadataTable3",
                    Arrays.asList(
                            Row.of("x8,8,1,1,file:" + file1.getPath()),
                            Row.of("x8,8,2,2,file:" + file2.getPath()),
                            Row.of("x8,8,3,3,file:" + file1.getPath())));
            check(
                    "select x,y,a,b,`flink.fs.path` from withMetadataTable5",
                    Arrays.asList(
                            // TODO: should path have a leading file:/ or not?
                            Row.of("x8,8,1,1,file:" + file1.getPath()),
                            Row.of("x8,8,2,2,file:" + file2.getPath()),
                            Row.of("x8,8,3,3,file:" + file1.getPath())));
        }

        @Test
        public void testBasenameMetadata() throws Exception {
            String path = new URI(resultPath()).getPath();
            new File(path).mkdirs();
            File file1 = new File(path, "test_file1");
            file1.createNewFile();
            FileUtils.writeFileUtf8(file1, "x8,8,1,1\n" + "x8,8,3,3");
            File file2 = new File(path, "test_file2");
            file2.createNewFile();
            FileUtils.writeFileUtf8(file2, "x8,8,2,2");

            check(
                    "select x,y,a,b,`flink.fs.basename` from withMetadataTable",
                    Arrays.asList(
                            Row.of("x8,8,1,1,test_file1"),
                            Row.of("x8,8,2,2,test_file2"),
                            Row.of("x8,8,3,3,test_file1")));
            check(
                    "select x,y,a,b,`flink.fs.basename` from withMetadataTable2",
                    Arrays.asList(
                            Row.of("x8,8,1,1,test_file1"),
                            Row.of("x8,8,2,2,test_file2"),
                            Row.of("x8,8,3,3,test_file1")));
            check(
                    "select x,y,a,b,`flink.fs.basename` from withMetadataTable4",
                    Arrays.asList(
                            Row.of("x8,8,1,1,test_file1"),
                            Row.of("x8,8,2,2,test_file2"),
                            Row.of("x8,8,3,3,test_file1")));
            check(
                    "select x,y,a,b,`flink.fs.basename` from withMetadataTable5",
                    Arrays.asList(
                            Row.of("x8,8,1,1,test_file1"),
                            Row.of("x8,8,2,2,test_file2"),
                            Row.of("x8,8,3,3,test_file1")));
        }

        @Test
        public void testMetadata() throws Exception {
            String path = new URI(resultPath()).getPath();
            new File(path).mkdirs();
            File file1 = new File(path, "test_file1");
            file1.createNewFile();
            FileUtils.writeFileUtf8(file1, "x1,2,3,4\n" + "x9,10,11,12");
            File file2 = new File(path, "test_file2");
            file2.createNewFile();
            FileUtils.writeFileUtf8(file2, "x5,6,7,8");

            //  x  y  a b
            // x1  2  3 4
            check(
                    "select x,`flink.fs.path`,a,`flink.fs.basename`,y from withMetadataTable", // we
                    // drop field b
                    Arrays.asList(
                            // TODO: should path have a leading file:/ or not?
                            Row.of("x1,file:" + file1.getAbsolutePath() + ",3,test_file1,2"),
                            Row.of("x5,file:" + file2.getAbsolutePath() + ",7,test_file2,6"),
                            Row.of("x9,file:" + file1.getAbsolutePath() + ",11,test_file1,10")));

            check(
                    "select x,`flink.fs.path`,b,a,`flink.fs.basename`,y from withMetadataTable",
                    Arrays.asList(
                            Row.of("x1,file:" + file1.getAbsolutePath() + ",4,3,test_file1,2"),
                            Row.of("x5,file:" + file2.getAbsolutePath() + ",8,7,test_file2,6"),
                            Row.of("x9,file:" + file1.getAbsolutePath() + ",12,11,test_file1,10")));

            check(
                    "select x,`flink.fs.path`,b,a,bname,y from withMetadataTable6",
                    Arrays.asList(
                            Row.of("x1,file:" + file1.getAbsolutePath() + ",4,3,test_file1,2"),
                            Row.of("x5,file:" + file2.getAbsolutePath() + ",8,7,test_file2,6"),
                            Row.of("x9,file:" + file1.getAbsolutePath() + ",12,11,test_file1,10")));

            check(
                    "select x,`flink.fs.path`,b,a,bname,y from withMetadataTable7",
                    Arrays.asList(
                            Row.of("x1,file:" + file1.getAbsolutePath() + ",4,3,test_file1,2"),
                            Row.of("x5,file:" + file2.getAbsolutePath() + ",8,7,test_file2,6"),
                            Row.of("x9,file:" + file1.getAbsolutePath() + ",12,11,test_file1,10")));
        }
    }
}
