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

package org.apache.flink.table.jdbc.utils;

import org.apache.flink.table.data.RowData;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Timestamp;
import java.util.Map;

/** Converter string value to different value. */
public class StringDataConverter implements DataConverter {
    public static final DataConverter CONVERTER = new StringDataConverter();

    private StringDataConverter() {}

    @Override
    public boolean getBoolean(RowData rowData, int pos) {
        return Boolean.parseBoolean(getString(rowData, pos));
    }

    @Override
    public byte getByte(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? 0 : Byte.parseByte(getString(rowData, pos));
    }

    @Override
    public short getShort(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? 0 : Short.parseShort(getString(rowData, pos));
    }

    @Override
    public int getInt(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? 0 : Integer.parseInt(getString(rowData, pos));
    }

    @Override
    public long getLong(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? 0 : Long.parseLong(getString(rowData, pos));
    }

    @Override
    public float getFloat(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? 0 : Float.parseFloat(getString(rowData, pos));
    }

    @Override
    public double getDouble(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? 0 : Double.parseDouble(getString(rowData, pos));
    }

    @Override
    public String getString(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? null : rowData.getString(pos).toString();
    }

    @Override
    public BigDecimal getDecimal(RowData rowData, int pos, int precision, int scale) {
        return rowData.isNullAt(pos)
                ? null
                : new BigDecimal(getString(rowData, pos)).setScale(scale);
    }

    @Override
    public byte[] getBinary(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? null : rowData.getString(pos).toBytes();
    }

    @Override
    public Timestamp getTimestamp(RowData rowData, int pos, int precision) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Array getArray(RowData rowData, int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<?, ?> getMap(RowData rowData, int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowData getRow(RowData rowData, int pos, int numFields) {
        throw new UnsupportedOperationException();
    }
}
