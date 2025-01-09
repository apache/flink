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

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** IT tests for verifying watermarks behaviour. */
class WatermarkITCase extends StreamingTestBase {

    @Test
    void testWatermarkNotMovingBack() {
        List<Row> data =
                Arrays.asList(
                        Row.of(1, LocalDateTime.parse("2024-01-01T00:00:00")),
                        Row.of(3, LocalDateTime.parse("2024-01-03T00:00:00")),
                        Row.of(2, LocalDateTime.parse("2024-01-02T00:00:00")));

        String dataId = TestValuesTableFactory.registerData(data);

        final String ddl =
                String.format(
                        "CREATE Table VirtualTable (\n"
                                + "  a INT,\n"
                                + "  c TIMESTAMP(3),\n"
                                + "  WATERMARK FOR c as c\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'false',\n"
                                + "  'scan.watermark.emit.strategy' = 'on-periodic',\n"
                                + "  'enable-watermark-push-down' = 'true',\n"
                                + "  'disable-lookup' = 'true',\n"
                                + "  'data-id' = '%s'\n"
                                + ")\n",
                        dataId);

        tEnv().executeSql(ddl);
        tEnv().getConfig().set(CoreOptions.DEFAULT_PARALLELISM, 1);
        String query = "SELECT a, c, current_watermark(c) FROM VirtualTable order by c";

        final List<Row> result = CollectionUtil.iteratorToList(tEnv().executeSql(query).collect());
        final List<String> actualWatermarks =
                TestValuesTableFactory.getWatermarkOutput("VirtualTable").stream()
                        .map(
                                x ->
                                        TimestampData.fromEpochMillis(x.getTimestamp())
                                                .toLocalDateTime()
                                                .toString())
                        .collect(Collectors.toList());

        // Underneath, we use FromElementSourceFunctionWithWatermark which is a SourceFunction.
        // SourceFunction does not support watermark moving back. SourceStreamTask does not support
        // WatermarkGenerator natively. The test implementation calls
        // WatermarkGenerator#onPeriodicEmit
        // after each record, which makes the test deterministic.
        // Additionally, the GeneratedWatermarkGeneratorSupplier does not deduplicate already
        // emitted
        // watermarks. This is usually handled by the target WatermarkOutput. In this test, we do
        // not deduplicate watermarks because we use TestValuesWatermarkOutput.
        // Given the fact watermarks are generated after every record and we don't deduplicate them,
        // we have "2024-01-03T00:00" twice in the expected watermarks.
        assertThat(actualWatermarks)
                .containsExactly("2024-01-01T00:00", "2024-01-03T00:00", "2024-01-03T00:00");
        assertThat(result)
                .containsExactly(
                        Row.of(
                                1,
                                LocalDateTime.parse("2024-01-01T00:00"),
                                LocalDateTime.parse("2024-01-01T00:00")),
                        Row.of(
                                2,
                                LocalDateTime.parse("2024-01-02T00:00"),
                                LocalDateTime.parse("2024-01-03T00:00")),
                        Row.of(
                                3,
                                LocalDateTime.parse("2024-01-03T00:00"),
                                LocalDateTime.parse("2024-01-03T00:00")));
    }
}
