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

package org.apache.flink.table.planner.runtime.stream.table;

import org.apache.flink.connector.print.table.PrintTableSinkFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** End to end tests for {@link PrintTableSinkFactory}. */
public class PrintConnectorITCase extends StreamingTestBase {

    private final PrintStream originalSystemOut = System.out;
    private final PrintStream originalSystemErr = System.err;

    private final ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
    private final ByteArrayOutputStream arrayErrorStream = new ByteArrayOutputStream();

    @Before
    public void setUp() {
        System.setOut(new PrintStream(arrayOutputStream));
        System.setErr(new PrintStream(arrayErrorStream));
    }

    @After
    public void tearDown() {
        if (System.out != originalSystemOut) {
            System.out.close();
        }
        if (System.err != originalSystemErr) {
            System.err.close();
        }
        System.setOut(originalSystemOut);
        System.setErr(originalSystemErr);
    }

    @Test
    public void testTypes() throws Exception {
        test(false);
    }

    @Test
    public void testStandardError() throws Exception {
        test(true);
    }

    @Test
    public void testWithParallelism() throws Exception {
        tEnv().executeSql(
                        "create table print_t ("
                                + "f0 int,"
                                + "f1 double"
                                + ") with ("
                                + "'connector' = 'print',"
                                + "'print-identifier' = 'test_print',"
                                + "'sink.parallelism' = '2',"
                                + "'standard-error'='false')");
        DataType type = tEnv().from("print_t").getResolvedSchema().toPhysicalRowDataType();
        Row row = Row.of(1, 1.1);
        tEnv().fromValues(type, Collections.singleton(row)).executeInsert("print_t").await();

        String expectedLine1 = "test_print:1> +I[" + /* 0 */ "1, " + /* 1 */ "1.1" + "]";
        String expectedLine2 = "test_print:2> +I[" + /* 0 */ "1, " + /* 1 */ "1.1" + "]";
        assertThat(
                        arrayOutputStream.toString().equals(expectedLine1 + "\n")
                                || arrayOutputStream.toString().equals(expectedLine2 + "\n"))
                .isTrue();
    }

    @Test
    public void testWithPartitionedTableAll() throws Exception {
        createPartitionedTable();
        tEnv().executeSql("INSERT INTO print_t PARTITION (f0=1,f1=1.1) SELECT 'n1'").await();

        String expectedLine1 =
                "test_print:f0=1:f1=1.1:1> +I["
                        + /* 0 */ "1, "
                        + /* 1 */ "1.1, "
                        + /* 2 */ "n1"
                        + "]";
        String expectedLine2 =
                "test_print:f0=1:f1=1.1:2> +I["
                        + /* 0 */ "1, "
                        + /* 1 */ "1.1, "
                        + /* 2 */ "n1"
                        + "]";
        assertThat(
                        arrayOutputStream.toString().equals(expectedLine1 + "\n")
                                || arrayOutputStream.toString().equals(expectedLine2 + "\n"))
                .isTrue();
    }

    @Test
    public void testWithPartitionedTablePart() throws Exception {
        createPartitionedTable();
        tEnv().executeSql("INSERT INTO print_t PARTITION (f0=1) SELECT 1.1, 'n1'").await();

        String expectedLine1 =
                "test_print:f0=1:1> +I[" + /* 0 */ "1, " + /* 1 */ "1.1, " + /* 2 */ "n1" + "]";
        String expectedLine2 =
                "test_print:f0=1:2> +I[" + /* 0 */ "1, " + /* 1 */ "1.1, " + /* 2 */ "n1" + "]";
        assertThat(
                        arrayOutputStream.toString().equals(expectedLine1 + "\n")
                                || arrayOutputStream.toString().equals(expectedLine2 + "\n"))
                .isTrue();
    }

    private void createPartitionedTable() {
        tEnv().executeSql(
                        "create table print_t ("
                                + "f0 int,"
                                + "f1 double,"
                                + "f2 string) "
                                + "PARTITIONED BY (f0, f1) "
                                + "with ("
                                + "'connector' = 'print',"
                                + "'print-identifier' = 'test_print',"
                                + "'sink.parallelism' = '2',"
                                + "'standard-error'='false')");
    }

    private void test(boolean standardError) throws Exception {
        tEnv().executeSql(
                        String.format(
                                "create table print_t ("
                                        + "f0 int,"
                                        + "f1 double,"
                                        + "f2 decimal(5, 2),"
                                        + "f3 boolean,"
                                        + "f4 string,"
                                        + "f5 date,"
                                        + "f6 time,"
                                        + "f7 timestamp,"
                                        + "f8 bytes,"
                                        + "f9 array<int>,"
                                        + "f10 map<int, int>,"
                                        + "f11 row<n0 int, n1 string>"
                                        + ") with ("
                                        + "'connector' = 'print',"
                                        + "'print-identifier' = '%s',"
                                        + "'standard-error'='%b')",
                                "test_print", standardError));
        DataType type = tEnv().from("print_t").getResolvedSchema().toPhysicalRowDataType();
        Map<Integer, Integer> mapData = new HashMap<>();
        mapData.put(1, 1);
        mapData.put(2, 2);
        Row row =
                Row.of(
                        /* 0 */ 1,
                        /* 1 */ 1.1,
                        /* 2 */ BigDecimal.valueOf(1.11),
                        /* 3 */ false,
                        /* 4 */ "f4",
                        /* 5 */ LocalDate.of(2020, 11, 5),
                        /* 6 */ LocalTime.of(12, 22, 35),
                        /* 7 */ LocalDateTime.of(2020, 11, 5, 12, 22, 35),
                        /* 8 */ new byte[] {1, 2, 3},
                        /* 9 */ new int[] {11, 22, 33},
                        /* 10 */ mapData,
                        /* 11 */ Row.of(1, "1"));
        tEnv().fromValues(type, Arrays.asList(row, row)).executeInsert("print_t").await();

        String expectedLine =
                "test_print> +I["
                        +
                        /* 0 */ "1, "
                        +
                        /* 1 */ "1.1, "
                        +
                        /* 2 */ "1.11, "
                        +
                        /* 3 */ "false, "
                        +
                        /* 4 */ "f4, "
                        +
                        /* 5 */ "2020-11-05, "
                        +
                        /* 6 */ "12:22:35, "
                        +
                        /* 7 */ "2020-11-05T12:22:35, "
                        +
                        /* 8 */ "[1, 2, 3], "
                        +
                        /* 9 */ "[11, 22, 33], "
                        +
                        /* 10 */ "{1=1, 2=2}, "
                        +
                        /* 11 */ "+I[1, 1]"
                        + "]";
        assertThat(standardError ? arrayErrorStream.toString() : arrayOutputStream.toString())
                .isEqualTo(expectedLine + "\n" + expectedLine + "\n");
    }
}
