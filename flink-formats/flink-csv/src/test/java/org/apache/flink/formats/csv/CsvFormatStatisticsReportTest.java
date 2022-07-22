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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.plan.stats.TableStats;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for statistics functionality in {@link CsvFormatFactory}. */
public class CsvFormatStatisticsReportTest {

    private static CsvFileFormatFactory.CsvBulkDecodingFormat csvBulkDecodingFormat;

    @BeforeEach
    public void setup() {
        Configuration configuration = new Configuration();
        csvBulkDecodingFormat = new CsvFileFormatFactory.CsvBulkDecodingFormat(configuration);
    }

    @Test
    public void testCsvFormatStatsReportWithSingleFile() throws IOException {
        String fileContent =
                "#description of the data\n"
                        + "header1|header2|header3|\n"
                        + "this is|1|2.0|\n"
                        + "//a comment\n"
                        + "a test|3|4.0|\n"
                        + "#next|5|6.0|\n";

        Path tempFile = createTempFile(fileContent);

        TableStats tableStats =
                csvBulkDecodingFormat.reportStatistics(Collections.singletonList(tempFile), null);
        assertThat(tableStats).isEqualTo(new TableStats(6));
    }

    @Test
    public void testCsvFormatStatsReportWithMultiFile() throws IOException {
        String fileContent1 =
                "#description of the data\r\n"
                        + "header1|header2|header3|\r\n"
                        + "this is|1|2.0|\r\n"
                        + "//a comment\r\n"
                        + "a test|3|4.0|\r\n"
                        + "#next|5|6.0|\r\n";
        String fileContent2 =
                "#description of the data\r\n"
                        + "header1|header2|header3|\r\n"
                        + "this is|1|2.0|\r\n"
                        + "//a comment\r\n"
                        + "a test|3|4.0|\r\n"
                        + "#next|5|6.0|\r\n";
        Path tempFile1 = createTempFile(fileContent1);
        Path tempFile2 = createTempFile(fileContent2);
        List<Path> files = new ArrayList<>();
        files.add(tempFile1);
        files.add(tempFile2);

        TableStats tableStats = csvBulkDecodingFormat.reportStatistics(files, null);
        assertThat(tableStats).isEqualTo(new TableStats(12));
    }

    @Test
    public void testRowSizeBiggerThanTotalSampleLineCnt() throws IOException {
        StringBuilder builder = new StringBuilder();
        int lineCnt = 1000;
        for (int i = 0; i < lineCnt; i++) {
            builder.append("header1|header2|header3|header4|header5").append("\n");
        }
        Path tempFile = createTempFile(builder.toString());
        TableStats tableStats =
                csvBulkDecodingFormat.reportStatistics(Collections.singletonList(tempFile), null);
        assertThat(tableStats).isEqualTo(new TableStats(lineCnt));
    }

    @Test
    public void testCsvFormatStatsReportWithEmptyFile() {
        TableStats tableStats = csvBulkDecodingFormat.reportStatistics(null, null);
        assertThat(tableStats).isEqualTo(TableStats.UNKNOWN);
    }

    private static Path createTempFile(String content) throws IOException {
        File tempFile = File.createTempFile("test_contents", "tmp");
        tempFile.deleteOnExit();
        OutputStreamWriter wrt =
                new OutputStreamWriter(
                        Files.newOutputStream(tempFile.toPath()), StandardCharsets.UTF_8);
        wrt.write(content);
        wrt.close();
        return new Path(tempFile.toURI().toString());
    }
}
