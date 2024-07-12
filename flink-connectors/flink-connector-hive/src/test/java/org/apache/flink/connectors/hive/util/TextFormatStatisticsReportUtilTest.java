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

package org.apache.flink.connectors.hive.util;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardOpenOption.APPEND;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link TextFormatStatisticsReportUtil}. */
class TextFormatStatisticsReportUtilTest {
    private Configuration hadoopConfig;
    private DataType producedDataType;

    @TempDir private java.nio.file.Path temporaryFolder;

    @BeforeEach
    void setUp() {
        hadoopConfig = new Configuration();
        // Create a sample producedDataType with a RowType
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("field1", new VarCharType()));
        fields.add(new RowType.RowField("field2", new VarCharType()));
        fields.add(new RowType.RowField("field3", new VarCharType()));
        producedDataType = new AtomicDataType(new RowType(fields));
    }

    @Test
    void testEstimateTableStatistics() throws IOException {
        // Create sample files for testing
        File tempFile = TempDirUtils.newFile(temporaryFolder, "flink_test_file.txt");

        List<Path> files = new ArrayList<>();
        files.add(new Path(tempFile.toURI()));

        String sampleString = "sample data";
        Files.write(tempFile.toPath(), sampleString.getBytes());
        TableStats stats =
                TextFormatStatisticsReportUtil.estimateTableStatistics(
                        files, producedDataType, hadoopConfig);
        assertEquals(1, stats.getRowCount());
        for (int i = 0; i < 10; ++i) {
            Files.write(tempFile.toPath(), sampleString.getBytes(), APPEND);
        }
        stats =
                TextFormatStatisticsReportUtil.estimateTableStatistics(
                        files, producedDataType, hadoopConfig);
        assertEquals(4, stats.getRowCount());
    }

    @Test
    void testEstimateFailedToUnknown() {
        List<Path> files = new ArrayList<>();
        files.add(new Path(URI.create("file:///non_existent_file.txt")));
        // Estimate table statistics
        TableStats stats =
                TextFormatStatisticsReportUtil.estimateTableStatistics(
                        files, producedDataType, hadoopConfig);
        // Verify that it returns TableStats.UNKNOWN on failure
        assertEquals(TableStats.UNKNOWN, stats);
    }
}
