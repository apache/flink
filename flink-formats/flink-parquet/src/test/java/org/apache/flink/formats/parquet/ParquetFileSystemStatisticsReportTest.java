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

package org.apache.flink.formats.parquet;

import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.concurrent.ExecutionException;

/**
 * Test for statistics functionality in {@link ParquetFileFormatFactory} in the case of file system
 * source.
 */
public class ParquetFileSystemStatisticsReportTest extends ParquetFormatStatisticsReportTest {

    @BeforeEach
    public void setup(@TempDir File file) throws Exception {
        super.setup(file);
    }

    @Test
    public void testParquetFileSystemStatisticsReport()
            throws ExecutionException, InterruptedException {
        // insert data and get statistics by get plan.
        DataType dataType = tEnv.from("sourceTable").getResolvedSchema().toPhysicalRowDataType();
        tEnv.fromValues(dataType, getData()).executeInsert("sourceTable").await();
        FlinkStatistic statistic = getStatisticsFromOptimizedPlan("select * from sourceTable");

        int expectedRowCount = 3;
        assertParquetFormatTableStatsEquals(statistic.getTableStats(), expectedRowCount, 1L);
    }
}
