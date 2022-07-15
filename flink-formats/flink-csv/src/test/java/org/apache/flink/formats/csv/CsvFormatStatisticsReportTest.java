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
import org.apache.flink.table.planner.utils.StatisticsReportTestBase;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for statistics functionality in {@link CsvFormatFactory}. */
public class CsvFormatStatisticsReportTest extends StatisticsReportTestBase {

    private static CsvFileFormatFactory.CsvBulkDecodingFormat csvBulkDecodingFormat;

    @BeforeEach
    public void setup(@TempDir File file) throws Exception {
        super.setup(file);
        createFileSystemSource();
        Configuration configuration = new Configuration();
        csvBulkDecodingFormat = new CsvFileFormatFactory.CsvBulkDecodingFormat(configuration);
    }

    @Override
    protected String[] properties() {
        List<String> ret = new ArrayList<>();
        ret.add("'format' = 'csv'");
        return ret.toArray(new String[0]);
    }

    @Test
    public void testCsvFormatStatsReportWithSingleFile() throws Exception {
        // insert data and get statistics.
        DataType dataType = tEnv.from("sourceTable").getResolvedSchema().toPhysicalRowDataType();
        tEnv.fromValues(dataType, getData()).executeInsert("sourceTable").await();
        assertThat(folder.listFiles()).isNotNull().hasSize(1);
        File[] files = folder.listFiles();
        assert files != null;
        TableStats tableStats =
                csvBulkDecodingFormat.reportStatistics(
                        Collections.singletonList(new Path(files[0].toURI().toString())), null);
        assertThat(tableStats).isEqualTo(new TableStats(3));
    }

    @Test
    public void testCsvFormatStatsReportWithMultiFile() throws Exception {
        // insert data and get statistics.
        DataType dataType = tEnv.from("sourceTable").getResolvedSchema().toPhysicalRowDataType();
        tEnv.fromValues(dataType, getData()).executeInsert("sourceTable").await();
        tEnv.fromValues(dataType, getData()).executeInsert("sourceTable").await();
        assertThat(folder.listFiles()).isNotNull().hasSize(2);
        File[] files = folder.listFiles();
        List<Path> paths = new ArrayList<>();
        assert files != null;
        paths.add(new Path(files[0].toURI().toString()));
        paths.add(new Path(files[1].toURI().toString()));
        TableStats tableStats = csvBulkDecodingFormat.reportStatistics(paths, null);
        assertThat(tableStats).isEqualTo(new TableStats(6));
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

    @Override
    protected Map<String, String> ddlTypesMap() {
        // now Csv format don't support type MAP, so we remove this type.
        Map<String, String> ddlTypes = super.ddlTypesMap();
        ddlTypes.remove("map<string, int>");
        return ddlTypes;
    }

    @Override
    protected Map<String, List<Object>> getDataMap() {
        // now Csv format don't support type MAP, so we remove data belong to this type.
        Map<String, List<Object>> dataMap = super.getDataMap();
        dataMap.remove("map<string, int>");
        return dataMap;
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
