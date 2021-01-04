/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

/** Unit test for TableUtil. */
public class TableUtilTest {
    @Rule public ExpectedException thrown = ExpectedException.none();
    private String[] colNames = new String[] {"f0", "f1", "f2"};
    private TableSchema tableSchema =
            new TableSchema(colNames, new TypeInformation[] {Types.INT, Types.LONG, Types.STRING});

    @Test
    public void testFindIndexFromName() {
        String[] colNames = new String[] {"f0", "f1", "F2"};
        Assert.assertEquals(0, TableUtil.findColIndex(colNames, "f0"));
        Assert.assertEquals(1, TableUtil.findColIndex(colNames, "F1"));
        Assert.assertEquals(-1, TableUtil.findColIndex(colNames, "f3"));
        Assert.assertEquals(0, TableUtil.findColIndex(tableSchema, "f0"));

        Assert.assertArrayEquals(
                new int[] {1, 2}, TableUtil.findColIndices(colNames, new String[] {"f1", "F2"}));
        Assert.assertArrayEquals(
                new int[] {1, 2}, TableUtil.findColIndices(tableSchema, new String[] {"f1", "F2"}));
        Assert.assertArrayEquals(
                new int[] {-1, 2},
                TableUtil.findColIndices(tableSchema, new String[] {"f3", "F2"}));
        Assert.assertArrayEquals(new int[] {0, 1, 2}, TableUtil.findColIndices(colNames, null));
    }

    @Test
    public void testFindTypeFromTable() {
        Assert.assertArrayEquals(
                new TypeInformation[] {TypeInformation.of(Integer.class), Types.LONG},
                TableUtil.findColTypes(tableSchema, new String[] {"f0", "f1"}));
        Assert.assertArrayEquals(
                new TypeInformation[] {Types.LONG, null},
                TableUtil.findColTypes(tableSchema, new String[] {"f1", "f3"}));
        Assert.assertArrayEquals(
                new TypeInformation[] {Types.INT, Types.LONG, Types.STRING},
                TableUtil.findColTypes(tableSchema, null));

        Assert.assertEquals(
                TypeInformation.of(Integer.class), TableUtil.findColType(tableSchema, "f0"));
        Assert.assertNull(TableUtil.findColType(tableSchema, "f3"));
    }

    @Test
    public void isNumberIsStringTest() {
        Assert.assertTrue(TableUtil.isSupportedNumericType(Types.INT));
        Assert.assertTrue(TableUtil.isSupportedNumericType(Types.DOUBLE));
        Assert.assertTrue(TableUtil.isSupportedNumericType(Types.LONG));
        Assert.assertTrue(TableUtil.isSupportedNumericType(Types.BYTE));
        Assert.assertTrue(TableUtil.isSupportedNumericType(Types.FLOAT));
        Assert.assertTrue(TableUtil.isSupportedNumericType(Types.SHORT));
        Assert.assertFalse(TableUtil.isSupportedNumericType(Types.STRING));
        Assert.assertTrue(TableUtil.isString(Types.STRING));
    }

    @Test
    public void assertColExistOrTypeTest() {
        String[] colNames = new String[] {"f0", "f1", "f2"};
        TableUtil.assertSelectedColExist(colNames, null);
        TableUtil.assertSelectedColExist(colNames, "f0");
        TableUtil.assertSelectedColExist(colNames, "f0", "f1");

        TableUtil.assertNumericalCols(tableSchema, null);
        TableUtil.assertNumericalCols(tableSchema, "f1");
        TableUtil.assertNumericalCols(tableSchema, "f0", "f1");

        TableUtil.assertStringCols(tableSchema, null);
        TableUtil.assertStringCols(tableSchema, "f2");

        TableUtil.assertVectorCols(tableSchema, null);
    }

    @Test
    public void assertColExistOrTypeExceptionTest() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(" col is not exist f3");
        TableUtil.assertSelectedColExist(colNames, "f3");

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(" col is not exist f3");
        TableUtil.assertSelectedColExist(colNames, "f0", "f3");

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("col type must be number f2");
        TableUtil.assertNumericalCols(tableSchema, "f2");

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("col type must be number f2");
        TableUtil.assertNumericalCols(tableSchema, "f2", "f0");

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("col type must be string f2");
        TableUtil.assertStringCols(tableSchema, "f2");

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("col type must be string f0");
        TableUtil.assertStringCols(tableSchema, "f0", "f3");
    }

    @Test
    public void getNumericColsTest() {
        TableSchema tableSchema =
                new TableSchema(
                        new String[] {"f0", "f1", "F2", "f3"},
                        new TypeInformation[] {Types.INT, Types.LONG, Types.STRING, Types.BOOLEAN});

        Assert.assertArrayEquals(new String[] {"f0", "f1"}, TableUtil.getNumericCols(tableSchema));
        Assert.assertArrayEquals(
                new String[] {"f1"}, TableUtil.getNumericCols(tableSchema, new String[] {"f0"}));
        Assert.assertArrayEquals(
                new String[] {"f0", "f1"},
                TableUtil.getNumericCols(tableSchema, new String[] {"f2"}));
    }

    @Test
    public void getCategoricalColsTest() {
        TableSchema tableSchema =
                new TableSchema(
                        new String[] {"f0", "f1", "f2", "f3"},
                        new TypeInformation[] {Types.INT, Types.LONG, Types.STRING, Types.BOOLEAN});

        Assert.assertArrayEquals(
                new String[] {"f2", "f3"},
                TableUtil.getCategoricalCols(tableSchema, tableSchema.getFieldNames(), null));
        Assert.assertArrayEquals(
                new String[] {"f2", "f0", "f3"},
                TableUtil.getCategoricalCols(
                        tableSchema, new String[] {"f2", "f1", "f0", "f3"}, new String[] {"f0"}));

        thrown.expect(IllegalArgumentException.class);
        Assert.assertArrayEquals(
                new String[] {"f3", "f2"},
                TableUtil.getCategoricalCols(
                        tableSchema, new String[] {"f3", "f0"}, new String[] {"f2"}));
    }

    @Test
    public void getStringColsTest() {
        TableSchema tableSchema =
                new TableSchema(
                        new String[] {"f0", "f1", "F2", "f3"},
                        new TypeInformation[] {Types.INT, Types.LONG, Types.STRING, Types.BOOLEAN});

        Assert.assertArrayEquals(new String[] {"F2"}, TableUtil.getStringCols(tableSchema));
        Assert.assertArrayEquals(
                new String[] {}, TableUtil.getStringCols(tableSchema, new String[] {"F2"}));
    }

    @Test
    public void formatTest() {
        TableSchema tableSchema =
                new TableSchema(
                        new String[] {"f0", "f1", "F2", "f3"},
                        new TypeInformation[] {Types.INT, Types.LONG, Types.STRING, Types.BOOLEAN});
        Row row = Row.of(1, 2L, "3", true);

        String format =
                TableUtil.format(tableSchema.getFieldNames(), Collections.singletonList(row));
        Assert.assertTrue(
                ("f0|f1|F2|f3\r\n" + "--|--|--|--\n" + "1|2|3|true").equalsIgnoreCase(format));
    }

    @Test
    public void testTempTable() {
        Assert.assertTrue(TableUtil.getTempTableName().startsWith("temp_"));
        Assert.assertFalse(TableUtil.getTempTableName().contains("-"));
    }
}
