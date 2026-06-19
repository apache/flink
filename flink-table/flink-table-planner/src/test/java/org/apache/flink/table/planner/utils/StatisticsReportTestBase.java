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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/** The base class for statistics report testing. */
public abstract class StatisticsReportTestBase extends TestLogger {

    private static final int DEFAULT_PARALLELISM = 4;

    protected TableEnvironment tEnv;
    protected File folder;

    @BeforeEach
    public void setup(@TempDir File file) throws Exception {
        folder = file;
        tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig()
                .getConfiguration()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM,
                        DEFAULT_PARALLELISM);
    }

    @AfterEach
    public void after() {
        TestValuesTableFactory.clearAllData();
    }

    protected void createFileSystemSource() {
        String ddl1 =
                String.format(
                        "CREATE TABLE sourceTable (\n"
                                + "%s"
                                + ") with (\n"
                                + " 'connector' = 'filesystem',"
                                + " 'path' = '%s',"
                                + "%s )",
                        String.join(",\n", ddlTypesMapToStringList(ddlTypesMap())),
                        folder,
                        String.join(",\n", properties()));
        tEnv.executeSql(ddl1);
    }

    protected abstract String[] properties();

    protected Map<String, String> ddlTypesMap() {
        Map<String, String> ddlTypesMap = new LinkedHashMap<>();
        ddlTypesMap.put("boolean", "f_boolean");
        ddlTypesMap.put("tinyint", "f_tinyint");
        ddlTypesMap.put("smallint", "f_smallint");
        ddlTypesMap.put("int", "f_int");
        ddlTypesMap.put("bigint", "f_bigint");
        ddlTypesMap.put("float", "f_float");
        ddlTypesMap.put("double", "f_double");
        ddlTypesMap.put("string", "f_string");
        ddlTypesMap.put("decimal(5,2)", "f_decimal5");
        ddlTypesMap.put("decimal(14,2)", "f_decimal14");
        ddlTypesMap.put("decimal(38,2)", "f_decimal38");
        ddlTypesMap.put("date", "f_date");
        ddlTypesMap.put("timestamp(3)", "f_timestamp3");
        ddlTypesMap.put("timestamp(9)", "f_timestamp9");
        ddlTypesMap.put("timestamp without time zone", "f_timestamp_wtz");
        ddlTypesMap.put("timestamp with local time zone", "f_timestamp_ltz");
        ddlTypesMap.put("binary(1)", "f_binary");
        ddlTypesMap.put("varbinary(1)", "f_varbinary");
        ddlTypesMap.put("time", "f_time");
        ddlTypesMap.put("row<col1 string, col2 int>", "f_row");
        ddlTypesMap.put("array<int>", "f_array");
        ddlTypesMap.put("map<string, int>", "f_map");

        return ddlTypesMap;
    }

    protected Map<String, List<Object>> getDataMap() {
        Map<String, List<Object>> dataMap = new LinkedHashMap<>();
        dataMap.put("boolean", Stream.of(null, true, false).collect(toList()));
        dataMap.put("tinyint", Stream.of((byte) 1, (byte) 2, (byte) 3).collect(toList()));
        dataMap.put("smallint", Stream.of(128, 111, 100).collect(toList()));
        dataMap.put("int", Stream.of(45536, null, 31000).collect(toList()));
        dataMap.put(
                "bigint",
                Stream.of(1238123899121L, 1238123899100L, 1238123899000L).collect(toList()));
        dataMap.put("float", Stream.of(33.333F, 33.311F, null).collect(toList()));
        dataMap.put("double", Stream.of(1.1D, 1.5D, 10.1D).collect(toList()));
        dataMap.put("string", Stream.of("abcd", "abcd", "def").collect(toList()));
        dataMap.put(
                "decimal(5,2)",
                Stream.of(
                                new BigDecimal("123.45"),
                                new BigDecimal("223.45"),
                                new BigDecimal("127.45"))
                        .collect(toList()));
        dataMap.put(
                "decimal(14,2)",
                Stream.of(
                                new BigDecimal("123333333333.33"),
                                new BigDecimal("123333333355.33"),
                                new BigDecimal("123333333344.33"))
                        .collect(toList()));
        dataMap.put(
                "decimal(38,2)",
                Stream.of(
                                new BigDecimal("123433343334333433343334333433343334.34"),
                                new BigDecimal("123433343334333433343334333433343334.33"),
                                null)
                        .collect(toList()));
        dataMap.put(
                "date",
                Stream.of(
                                LocalDate.parse("1990-10-14"),
                                LocalDate.parse("1990-10-15"),
                                LocalDate.parse("1990-10-16"))
                        .collect(toList()));
        dataMap.put(
                "timestamp(3)",
                Stream.of(
                                localDateTime("1990-10-14 12:12:43.123", 3),
                                localDateTime("1990-10-15 12:12:43.123", 3),
                                localDateTime("1990-10-16 12:12:43.123", 3))
                        .collect(toList()));
        dataMap.put(
                "timestamp(9)",
                Stream.of(
                                localDateTime("1990-10-14 12:12:43.123456789", 9),
                                localDateTime("1990-10-15 12:12:43.123456789", 9),
                                localDateTime("1990-10-16 12:12:43.123456789", 9))
                        .collect(toList()));
        dataMap.put(
                "timestamp without time zone",
                Stream.of(
                                localDateTime("1990-10-14 12:12:43.123", 3),
                                localDateTime("1990-10-15 12:12:43.123", 3),
                                localDateTime("1990-10-16 12:12:43.123", 3))
                        .collect(toList()));
        dataMap.put(
                "timestamp with local time zone",
                Stream.of(
                                localDateTime("1990-10-14 12:12:43.123", 3),
                                null,
                                localDateTime("1990-10-16 12:12:43.123", 3))
                        .collect(toList()));
        dataMap.put(
                "binary(1)",
                Stream.of(new byte[] {1}, new byte[] {2}, new byte[] {3}).collect(toList()));
        dataMap.put(
                "varbinary(1)", Stream.of(new byte[] {1}, null, new byte[] {3}).collect(toList()));
        dataMap.put(
                "time",
                Stream.of(
                                LocalTime.parse("12:12:43"),
                                LocalTime.parse("12:12:44"),
                                LocalTime.parse("12:12:45"))
                        .collect(toList()));
        dataMap.put(
                "row<col1 string, col2 int>",
                Stream.of(
                                new Object[] {"abc", 10},
                                new Object[] {"ef", 11},
                                new Object[] {"gh", 12})
                        .collect(toList()));
        dataMap.put(
                "array<int>",
                Stream.of(new Object[] {1, 2, 3}, new Object[] {2, 3, 4}, new Object[] {3, 4, 5})
                        .collect(toList()));
        dataMap.put(
                "map<string, int>",
                Stream.of(
                                new Object[] {"abc", 10},
                                new Object[] {"ef", 11},
                                new Object[] {"gh", 12})
                        .collect(toList()));
        return dataMap;
    }

    protected List<Row> getData() {
        Map<String, List<Object>> dataMap = getDataMap();
        Row row1 = new Row(dataMap.size());
        Row row2 = new Row(dataMap.size());
        Row row3 = new Row(dataMap.size());
        int i = 0;
        for (Map.Entry<String, List<Object>> entry : dataMap.entrySet()) {
            List<Object> value = entry.getValue();
            switch (entry.getKey()) {
                case "row<col1 string, col2 int>":
                    row1.setField(i, getTypeRowData(value.get(0)));
                    row2.setField(i, getTypeRowData(value.get(1)));
                    row2.setField(i++, getTypeRowData(value.get(2)));
                    break;
                case "array<int>":
                    row1.setField(i, getTypeArrayData(value.get(0)));
                    row2.setField(i, getTypeArrayData(value.get(1)));
                    row2.setField(i++, getTypeArrayData(value.get(2)));
                    break;
                case "map<string, int>":
                    row1.setField(i, getTypeMapData(value.get(0)));
                    row2.setField(i, getTypeMapData(value.get(1)));
                    row2.setField(i++, getTypeMapData(value.get(2)));
                    break;
                default:
                    row1.setField(i, value.get(0));
                    row2.setField(i, value.get(1));
                    row3.setField(i++, value.get(2));
                    break;
            }
        }
        return Stream.of(row1, row2, row3).collect(toList());
    }

    private Row getTypeRowData(Object object) {
        Object[] objectList = (Object[]) object;
        Row row = new Row(objectList.length);
        row.setField(0, objectList[0]);
        row.setField(1, objectList[1]);
        return row;
    }

    private Map<String, Integer> getTypeMapData(Object object) {
        Object[] objectList = (Object[]) object;
        Map<String, Integer> map = new HashMap<>();
        map.put((String) objectList[0], (int) objectList[1]);
        return map;
    }

    private List<Integer> getTypeArrayData(Object object) {
        Object[] objectList = (Object[]) object;
        List<Integer> list = new ArrayList<>();
        for (Object obj : objectList) {
            list.add((int) obj);
        }
        return list;
    }

    protected FlinkStatistic getStatisticsFromOptimizedPlan(String sql) {
        RelNode relNode = TableTestUtil.toRelNode(tEnv.sqlQuery(sql));
        RelNode optimized = getPlanner(tEnv).optimize(relNode);
        FlinkStatisticVisitor visitor = new FlinkStatisticVisitor();
        visitor.go(optimized);
        return visitor.result;
    }

    private static class FlinkStatisticVisitor extends RelVisitor {
        private FlinkStatistic result = null;

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            if (node instanceof TableScan) {
                Preconditions.checkArgument(result == null);
                TableSourceTable table = (TableSourceTable) node.getTable();
                result = table.getStatistic();
            }
            super.visit(node, ordinal, parent);
        }
    }

    protected static String[] ddlTypesMapToStringList(Map<String, String> ddlTypesMap) {
        String[] types = new String[ddlTypesMap.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : ddlTypesMap.entrySet()) {
            String str = entry.getValue() + " " + entry.getKey();
            types[i++] = str;
        }
        return types;
    }

    private static PlannerBase getPlanner(TableEnvironment tEnv) {
        TableEnvironmentImpl tEnvImpl = (TableEnvironmentImpl) tEnv;
        return (PlannerBase) tEnvImpl.getPlanner();
    }

    protected LocalDateTime localDateTime(String dateTime, int precision) {
        return DateTimeUtils.parseTimestampData(dateTime, precision).toLocalDateTime();
    }
}
