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
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** ITCase to test csv format for {@link CsvFileFormatFactory} in batch mode. */
@ExtendWith(NoOpTestExtension.class)
class CsvFilesystemBatchITCase {

    /** General IT cases for CsvRowDataFilesystem in batch mode. */
    @Nested
    class GeneralCsvFilesystemBatchITCase extends BatchFileSystemITCaseBase {

        @Override
        public boolean supportsReadingMetadata() {
            return false;
        }

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
    @Nested
    class EnrichedCsvFilesystemBatchITCase extends BatchFileSystemITCaseBase {

        @Override
        public boolean supportsReadingMetadata() {
            return false;
        }

        @Override
        public String[] formatProperties() {
            List<String> ret = new ArrayList<>();
            ret.add("'format'='csv'");
            ret.add("'csv.ignore-parse-errors'='true'");
            ret.add("'csv.escape-character'='\t'");
            return ret.toArray(new String[0]);
        }

        @Test
        void testParseError() throws Exception {
            String path = new URI(resultPath()).getPath();
            new File(path).mkdirs();
            File file = new File(path, "test_file");
            file.createNewFile();
            FileUtils.writeFileUtf8(
                    file, "x5,5,1,1\n" + "x5,5,2,2,2\n" + "x5,5,3,3,3,3\n" + "x5,5,1,1");

            check(
                    "select * from nonPartitionedTable",
                    Arrays.asList(Row.of("x5", 5, 1, 1), Row.of("x5", 5, 1, 1)));
        }

        @Test
        void testParseErrorLast() throws Exception {
            String path = new URI(resultPath()).getPath();
            new File(path).mkdirs();
            File file = new File(path, "test_file");
            file.createNewFile();
            FileUtils.writeFileUtf8(
                    file, "x5,5,1,1\n" + "x5,5,2,2,2\n" + "x5,5,1,1\n" + "x5,5,3,3,3,3\n");

            check(
                    "select * from nonPartitionedTable",
                    Arrays.asList(Row.of("x5", 5, 1, 1), Row.of("x5", 5, 1, 1)));
        }

        @Test
        void testEscapeChar() throws Exception {
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
        public void testEmpty() throws Exception {
            String path = new URI(resultPath()).getPath();
            new File(path).mkdirs();
            File file = new File(path, "test_file");
            file.createNewFile();

            check("select * from nonPartitionedTable", Collections.emptyList());
        }
    }
}
