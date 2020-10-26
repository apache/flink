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

package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

import org.junit.Test;
import org.mockito.Mockito;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.PGobject;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;

/** Tests for the {@link PostgresRowConverter}. */
public class PostgresRowConverterTest {
    @Test
    public void testString() throws Exception {
        List<RowField> rowFields = new ArrayList<>();
        rowFields.add(
                new RowField(
                        "string", DataTypes.VARCHAR(Integer.MAX_VALUE).notNull().getLogicalType()));
        RowType rowType = new RowType(rowFields);
        PostgresRowConverter converter = new PostgresRowConverter(rowType);

        ResultSet rs = Mockito.mock(ResultSet.class);
        doReturn("test").when(rs).getObject(1);

        RowData result = converter.toInternal(rs);
        assertEquals("test", result.getString(0).toString());
    }

    @Test
    public void testNullString() throws Exception {
        List<RowField> rowFields = new ArrayList<>();
        rowFields.add(
                new RowField(
                        "string",
                        DataTypes.VARBINARY(Integer.MAX_VALUE).nullable().getLogicalType()));
        RowType rowType = new RowType(rowFields);
        PostgresRowConverter converter = new PostgresRowConverter(rowType);

        ResultSet rs = Mockito.mock(ResultSet.class);
        doReturn(null).when(rs).getObject(1);

        RowData result = converter.toInternal(rs);
        assertNull(result.getString(0));
    }

    @Test
    public void testBinary() throws Exception {
        List<RowField> rowFields = new ArrayList<>();
        rowFields.add(
                new RowField(
                        "binary",
                        DataTypes.ARRAY(DataTypes.VARBINARY(Integer.MAX_VALUE)).getLogicalType()));
        RowType rowType = new RowType(rowFields);
        PostgresRowConverter converter = new PostgresRowConverter(rowType);

        ResultSet rs = Mockito.mock(ResultSet.class);
        PgArray pgArray = Mockito.mock(PgArray.class);
        PGobject pgObject = new PGobject();
        pgObject.setValue("test");
        doReturn(new PGobject[] {pgObject}).when(pgArray).getArray();
        doReturn(pgArray).when(rs).getObject(1);

        RowData result = converter.toInternal(rs);
        assertArrayEquals("test".getBytes(StandardCharsets.UTF_8), result.getArray(0).getBinary(0));
    }

    @Test
    public void testArray() throws Exception {
        List<RowField> rowFields = new ArrayList<>();
        rowFields.add(
                new RowField(
                        "array",
                        DataTypes.ARRAY(DataTypes.VARCHAR(Integer.MAX_VALUE).notNull())
                                .notNull()
                                .getLogicalType()));
        RowType rowType = new RowType(rowFields);
        PostgresRowConverter converter = new PostgresRowConverter(rowType);

        ResultSet rs = Mockito.mock(ResultSet.class);
        PgArray pgArray = Mockito.mock(PgArray.class);
        doReturn(new String[] {"foo"}).when(pgArray).getArray();
        doReturn(pgArray).when(rs).getObject(1);

        RowData result = converter.toInternal(rs);
        assertEquals("foo", result.getArray(0).getString(0).toString());
    }

    @Test
    public void testNullArray() throws Exception {
        List<RowField> rowFields = new ArrayList<>();
        rowFields.add(
                new RowField(
                        "nullableArray",
                        DataTypes.ARRAY(DataTypes.VARCHAR(Integer.MAX_VALUE).notNull())
                                .nullable()
                                .getLogicalType()));
        RowType rowType = new RowType(rowFields);
        PostgresRowConverter converter = new PostgresRowConverter(rowType);

        ResultSet rs = Mockito.mock(ResultSet.class);
        doReturn(null).when(rs).getObject(1);

        RowData result = converter.toInternal(rs);
        assertNull(result.getArray(0));
    }
}
