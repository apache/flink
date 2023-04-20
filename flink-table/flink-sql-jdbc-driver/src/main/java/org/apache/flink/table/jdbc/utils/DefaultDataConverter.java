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

/** Default data converter for result set. */
public class DefaultDataConverter implements DataConverter {
    public static final DataConverter CONVERTER = new DefaultDataConverter();

    private DefaultDataConverter() {}

    @Override
    public boolean getBoolean(RowData rowData, int pos) {
        return !rowData.isNullAt(pos) && rowData.getBoolean(pos);
    }

    @Override
    public byte getByte(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? 0 : rowData.getByte(pos);
    }

    @Override
    public short getShort(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? 0 : rowData.getShort(pos);
    }

    @Override
    public int getInt(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? 0 : rowData.getInt(pos);
    }

    @Override
    public long getLong(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? 0 : rowData.getLong(pos);
    }

    @Override
    public float getFloat(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? 0 : rowData.getFloat(pos);
    }

    @Override
    public double getDouble(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? 0 : rowData.getDouble(pos);
    }

    @Override
    public String getString(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? null : rowData.getString(pos).toString();
    }

    @Override
    public BigDecimal getDecimal(RowData rowData, int pos, int precision, int scale) {
        return rowData.isNullAt(pos)
                ? null
                : rowData.getDecimal(pos, precision, scale).toBigDecimal();
    }

    @Override
    public Timestamp getTimestamp(RowData rowData, int pos, int precision) {
        return rowData.isNullAt(pos) ? null : rowData.getTimestamp(pos, precision).toTimestamp();
    }

    @Override
    public byte[] getBinary(RowData rowData, int pos) {
        return rowData.isNullAt(pos) ? null : rowData.getBinary(pos);
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
        return rowData.getRow(pos, numFields);
    }
}
