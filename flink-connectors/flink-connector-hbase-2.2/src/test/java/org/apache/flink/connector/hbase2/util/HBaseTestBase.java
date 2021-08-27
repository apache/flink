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

package org.apache.flink.connector.hbase2.util;

import org.apache.flink.table.api.EnvironmentSettings;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.functions.SqlDateTimeUtils.dateToInternal;
import static org.apache.flink.table.runtime.functions.SqlDateTimeUtils.timeToInternal;
import static org.apache.flink.table.runtime.functions.SqlDateTimeUtils.timestampToInternal;

/** Abstract IT case class for HBase. */
public abstract class HBaseTestBase extends HBaseTestingClusterAutoStarter {

    protected static final String TEST_TABLE_1 = "testTable1";
    protected static final String TEST_TABLE_2 = "testTable2";
    protected static final String TEST_TABLE_3 = "testTable3";

    protected static final String ROW_KEY = "rowkey";

    protected static final String FAMILY1 = "family1";
    protected static final String F1COL1 = "col1";

    protected static final String FAMILY2 = "family2";
    protected static final String F2COL1 = "col1";
    protected static final String F2COL2 = "col2";

    protected static final String FAMILY3 = "family3";
    protected static final String F3COL1 = "col1";
    protected static final String F3COL2 = "col2";
    protected static final String F3COL3 = "col3";

    protected static final String FAMILY4 = "family4";
    protected static final String F4COL1 = "col1";
    protected static final String F4COL2 = "col2";
    protected static final String F4COL3 = "col3";
    protected static final String F4COL4 = "col4";

    private static final byte[][] FAMILIES =
            new byte[][] {
                Bytes.toBytes(FAMILY1),
                Bytes.toBytes(FAMILY2),
                Bytes.toBytes(FAMILY3),
                Bytes.toBytes(FAMILY4)
            };

    private static final byte[][] SPLIT_KEYS = new byte[][] {Bytes.toBytes(4)};

    protected EnvironmentSettings streamSettings;
    protected EnvironmentSettings batchSettings;

    @BeforeClass
    public static void activateHBaseCluster() throws IOException {
        prepareTables();
    }

    @Before
    public void before() {
        this.streamSettings = EnvironmentSettings.inStreamingMode();
        this.batchSettings = EnvironmentSettings.inBatchMode();
    }

    private static void prepareTables() throws IOException {
        createHBaseTable1();
        createHBaseTable2();
        createHBaseTable3();
    }

    private static void createHBaseTable1() throws IOException {
        // create a table
        TableName tableName = TableName.valueOf(TEST_TABLE_1);
        createTable(tableName, FAMILIES, SPLIT_KEYS);

        // get the HTable instance
        Table table = openTable(tableName);
        List<Put> puts = new ArrayList<>();
        // add some data
        puts.add(
                putRow(
                        1,
                        10,
                        "Hello-1",
                        100L,
                        1.01,
                        false,
                        "Welt-1",
                        Timestamp.valueOf("2019-08-18 19:00:00"),
                        Date.valueOf("2019-08-18"),
                        Time.valueOf("19:00:00"),
                        new BigDecimal("12345678.0001")));
        puts.add(
                putRow(
                        2,
                        20,
                        "Hello-2",
                        200L,
                        2.02,
                        true,
                        "Welt-2",
                        Timestamp.valueOf("2019-08-18 19:01:00"),
                        Date.valueOf("2019-08-18"),
                        Time.valueOf("19:01:00"),
                        new BigDecimal("12345678.0002")));
        puts.add(
                putRow(
                        3,
                        30,
                        "Hello-3",
                        300L,
                        3.03,
                        false,
                        "Welt-3",
                        Timestamp.valueOf("2019-08-18 19:02:00"),
                        Date.valueOf("2019-08-18"),
                        Time.valueOf("19:02:00"),
                        new BigDecimal("12345678.0003")));
        puts.add(
                putRow(
                        4,
                        40,
                        null,
                        400L,
                        4.04,
                        true,
                        "Welt-4",
                        Timestamp.valueOf("2019-08-18 19:03:00"),
                        Date.valueOf("2019-08-18"),
                        Time.valueOf("19:03:00"),
                        new BigDecimal("12345678.0004")));
        puts.add(
                putRow(
                        5,
                        50,
                        "Hello-5",
                        500L,
                        5.05,
                        false,
                        "Welt-5",
                        Timestamp.valueOf("2019-08-19 19:10:00"),
                        Date.valueOf("2019-08-19"),
                        Time.valueOf("19:10:00"),
                        new BigDecimal("12345678.0005")));
        puts.add(
                putRow(
                        6,
                        60,
                        "Hello-6",
                        600L,
                        6.06,
                        true,
                        "Welt-6",
                        Timestamp.valueOf("2019-08-19 19:20:00"),
                        Date.valueOf("2019-08-19"),
                        Time.valueOf("19:20:00"),
                        new BigDecimal("12345678.0006")));
        puts.add(
                putRow(
                        7,
                        70,
                        "Hello-7",
                        700L,
                        7.07,
                        false,
                        "Welt-7",
                        Timestamp.valueOf("2019-08-19 19:30:00"),
                        Date.valueOf("2019-08-19"),
                        Time.valueOf("19:30:00"),
                        new BigDecimal("12345678.0007")));
        puts.add(
                putRow(
                        8,
                        80,
                        null,
                        800L,
                        8.08,
                        true,
                        "Welt-8",
                        Timestamp.valueOf("2019-08-19 19:40:00"),
                        Date.valueOf("2019-08-19"),
                        Time.valueOf("19:40:00"),
                        new BigDecimal("12345678.0008")));

        // append rows to table
        table.put(puts);
        table.close();
    }

    private static void createHBaseTable2() {
        // create a table
        TableName tableName = TableName.valueOf(TEST_TABLE_2);
        createTable(tableName, FAMILIES, SPLIT_KEYS);
    }

    private static void createHBaseTable3() {
        // create a table
        byte[][] families =
                new byte[][] {
                    Bytes.toBytes(FAMILY1),
                    Bytes.toBytes(FAMILY2),
                    Bytes.toBytes(FAMILY3),
                    Bytes.toBytes(FAMILY4),
                };
        TableName tableName = TableName.valueOf(TEST_TABLE_3);
        createTable(tableName, families, SPLIT_KEYS);
    }

    private static Put putRow(
            int rowKey,
            int f1c1,
            String f2c1,
            long f2c2,
            double f3c1,
            boolean f3c2,
            String f3c3,
            Timestamp f4c1,
            Date f4c2,
            Time f4c3,
            BigDecimal f4c4) {
        Put put = new Put(Bytes.toBytes(rowKey));
        // family 1
        put.addColumn(Bytes.toBytes(FAMILY1), Bytes.toBytes(F1COL1), Bytes.toBytes(f1c1));
        // family 2
        if (f2c1 != null) {
            put.addColumn(Bytes.toBytes(FAMILY2), Bytes.toBytes(F2COL1), Bytes.toBytes(f2c1));
        }
        put.addColumn(Bytes.toBytes(FAMILY2), Bytes.toBytes(F2COL2), Bytes.toBytes(f2c2));
        // family 3
        put.addColumn(Bytes.toBytes(FAMILY3), Bytes.toBytes(F3COL1), Bytes.toBytes(f3c1));
        put.addColumn(Bytes.toBytes(FAMILY3), Bytes.toBytes(F3COL2), Bytes.toBytes(f3c2));
        put.addColumn(Bytes.toBytes(FAMILY3), Bytes.toBytes(F3COL3), Bytes.toBytes(f3c3));

        // family 4
        put.addColumn(
                Bytes.toBytes(FAMILY4),
                Bytes.toBytes(F4COL1),
                Bytes.toBytes(timestampToInternal(f4c1)));
        put.addColumn(
                Bytes.toBytes(FAMILY4), Bytes.toBytes(F4COL2), Bytes.toBytes(dateToInternal(f4c2)));
        put.addColumn(
                Bytes.toBytes(FAMILY4), Bytes.toBytes(F4COL3), Bytes.toBytes(timeToInternal(f4c3)));
        put.addColumn(Bytes.toBytes(FAMILY4), Bytes.toBytes(F4COL4), Bytes.toBytes(f4c4));
        return put;
    }
}
