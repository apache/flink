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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.variant.Variant;

/** A row that repeats the columns of a given row by the given count. */
public class RepeatedRowData implements RowData {

    private final int count;
    private RowData row;

    public RepeatedRowData(int count) {
        this.count = count;
    }

    /**
     * Replaces the {@link RowData} backing this {@link RepeatedRowData}.
     *
     * <p>This method replaces the backing rows in place and does not return a new object. This is
     * done for performance reasons.
     */
    public RepeatedRowData replace(RowData row) {
        this.row = row;
        return this;
    }

    @Override
    public int getArity() {
        return row.getArity() * count;
    }

    @Override
    public RowKind getRowKind() {
        return row.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        row.setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int pos) {
        return row.isNullAt(pos / count);
    }

    @Override
    public boolean getBoolean(int pos) {
        return row.getBoolean(pos / count);
    }

    @Override
    public byte getByte(int pos) {
        return row.getByte(pos / count);
    }

    @Override
    public short getShort(int pos) {
        return row.getShort(pos / count);
    }

    @Override
    public int getInt(int pos) {
        return row.getInt(pos / count);
    }

    @Override
    public long getLong(int pos) {
        return row.getLong(pos / count);
    }

    @Override
    public float getFloat(int pos) {
        return row.getFloat(pos / count);
    }

    @Override
    public double getDouble(int pos) {
        return row.getDouble(pos / count);
    }

    @Override
    public StringData getString(int pos) {
        return row.getString(pos / count);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return row.getDecimal(pos / count, precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return row.getTimestamp(pos / count, precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return row.getRawValue(pos / count);
    }

    @Override
    public byte[] getBinary(int pos) {
        return row.getBinary(pos / count);
    }

    @Override
    public ArrayData getArray(int pos) {
        return row.getArray(pos / count);
    }

    @Override
    public MapData getMap(int pos) {
        return row.getMap(pos / count);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return row.getRow(pos / count, numFields);
    }

    @Override
    public Variant getVariant(int pos) {
        return row.getVariant(pos / count);
    }
}
