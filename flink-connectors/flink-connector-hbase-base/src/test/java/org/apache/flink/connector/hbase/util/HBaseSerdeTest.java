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

package org.apache.flink.connector.hbase.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test for {@link HBaseSerde}. */
public class HBaseSerdeTest {

    private static final String ROW_KEY = "rowkey";

    private static final String FAMILY1 = "family1";
    private static final String F1COL1 = "col1";

    private static final String FAMILY2 = "family2";
    private static final String F2COL1 = "col1";
    private static final String F2COL2 = "col2";

    private static final String FAMILY3 = "family3";
    private static final String F3COL1 = "col1";
    private static final String F3COL2 = "col2";
    private static final String F3COL3 = "col3";

    @Test
    public void convertToNewRowTest() {
        HBaseSerde serde = createHBaseSerde();
        List<List<Cell>> cellsList = prepareCells();
        List<RowData> resultRowDatas = new ArrayList<>();
        List<String> resultRowDataStr = new ArrayList<>();
        for (List<Cell> cells : cellsList) {
            RowData row = serde.convertToNewRow(Result.create(cells));
            resultRowDatas.add(row);
            resultRowDataStr.add(row.toString());
        }

        // this verifies RowData is not reused
        assertFalse(resultRowDatas.get(0) == resultRowDatas.get(1));

        List<String> expected = new ArrayList<>();
        expected.add("+I(1,+I(10),+I(Hello-1,100),+I(1.01,false,Welt-1))");
        expected.add("+I(2,+I(20),+I(Hello-2,200),+I(2.02,true,Welt-2))");
        assertEquals(expected, resultRowDataStr);
    }

    @Test
    public void convertToReusedRowTest() {
        HBaseSerde serde = createHBaseSerde();
        List<List<Cell>> cellsList = prepareCells();
        List<RowData> resultRowDatas = new ArrayList<>();
        List<String> resultRowDataStr = new ArrayList<>();
        for (List<Cell> cells : cellsList) {
            RowData row = serde.convertToReusedRow(Result.create(cells));
            resultRowDatas.add(row);
            resultRowDataStr.add(row.toString());
        }

        // this verifies RowData is reused
        assertTrue(resultRowDatas.get(0) == resultRowDatas.get(1));

        List<String> expected = new ArrayList<>();
        expected.add("+I(1,+I(10),+I(Hello-1,100),+I(1.01,false,Welt-1))");
        expected.add("+I(2,+I(20),+I(Hello-2,200),+I(2.02,true,Welt-2))");
        assertEquals(expected, resultRowDataStr);
    }

    private HBaseSerde createHBaseSerde() {
        TableSchema schema =
                TableSchema.builder()
                        .field(ROW_KEY, DataTypes.INT())
                        .field(FAMILY1, DataTypes.ROW(DataTypes.FIELD(F1COL1, DataTypes.INT())))
                        .field(
                                FAMILY2,
                                DataTypes.ROW(
                                        DataTypes.FIELD(F2COL1, DataTypes.STRING()),
                                        DataTypes.FIELD(F2COL2, DataTypes.BIGINT())))
                        .field(
                                FAMILY3,
                                DataTypes.ROW(
                                        DataTypes.FIELD(F3COL1, DataTypes.DOUBLE()),
                                        DataTypes.FIELD(F3COL2, DataTypes.BOOLEAN()),
                                        DataTypes.FIELD(F3COL3, DataTypes.STRING())))
                        .build();
        HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(schema);
        return new HBaseSerde(hbaseSchema, "null");
    }

    private List<List<Cell>> prepareCells() {
        List<List<Cell>> cellList = new ArrayList<>();
        byte[] fam1 = Bytes.toBytes(FAMILY1);
        byte[] f1c1 = Bytes.toBytes(F1COL1);

        byte[] fam2 = Bytes.toBytes(FAMILY2);
        byte[] f2c1 = Bytes.toBytes(F2COL1);
        byte[] f2c2 = Bytes.toBytes(F2COL2);

        byte[] fam3 = Bytes.toBytes(FAMILY3);
        byte[] f3c1 = Bytes.toBytes(F3COL1);
        byte[] f3c2 = Bytes.toBytes(F3COL2);
        byte[] f3c3 = Bytes.toBytes(F3COL3);

        byte[] row1 = Bytes.toBytes(1);
        byte[] row2 = Bytes.toBytes(2);

        Cell kv111 = new KeyValue(row1, fam1, f1c1, Bytes.toBytes(10));
        Cell kv121 = new KeyValue(row1, fam2, f2c1, Bytes.toBytes("Hello-1"));
        Cell kv122 = new KeyValue(row1, fam2, f2c2, Bytes.toBytes(100L));
        Cell kv131 = new KeyValue(row1, fam3, f3c1, Bytes.toBytes(1.01));
        Cell kv132 = new KeyValue(row1, fam3, f3c2, Bytes.toBytes(false));
        Cell kv133 = new KeyValue(row1, fam3, f3c3, Bytes.toBytes("Welt-1"));

        Cell kv211 = new KeyValue(row2, fam1, f1c1, Bytes.toBytes(20));
        Cell kv221 = new KeyValue(row2, fam2, f2c1, Bytes.toBytes("Hello-2"));
        Cell kv222 = new KeyValue(row2, fam2, f2c2, Bytes.toBytes(200L));
        Cell kv231 = new KeyValue(row2, fam3, f3c1, Bytes.toBytes(2.02));
        Cell kv232 = new KeyValue(row2, fam3, f3c2, Bytes.toBytes(true));
        Cell kv233 = new KeyValue(row2, fam3, f3c3, Bytes.toBytes("Welt-2"));
        List<Cell> cells1 = new ArrayList<>();
        cells1.add(kv111);
        cells1.add(kv121);
        cells1.add(kv122);
        cells1.add(kv131);
        cells1.add(kv132);
        cells1.add(kv133);
        List<Cell> cells2 = new ArrayList<>();
        cells2.add(kv211);
        cells2.add(kv221);
        cells2.add(kv222);
        cells2.add(kv231);
        cells2.add(kv232);
        cells2.add(kv233);
        cellList.add(cells1);
        cellList.add(cells2);
        return cellList;
    }
}
