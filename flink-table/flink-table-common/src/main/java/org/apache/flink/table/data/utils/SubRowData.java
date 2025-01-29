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

package org.apache.flink.table.data.utils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import java.util.Objects;

/** An implementation of {@link RowData} which projects a source {@link RowData} to a subset. */
@PublicEvolving
public class SubRowData implements RowData {

    private final RowData rowData;
    private final int startIndex;
    private final int endIndex;

    /**
     * Construct a {@link RowData} which represents a subset of a given source {@link RowData}.
     *
     * <p>The range is inclusive on the left, and exclusive on the right. This means that the value
     * at {@param startIndexInclusive} in the source row is the first value in the returned row, but
     * the value at {@param endIndexExclusive} is not part of the returned {@link RowData}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * // 1, 2, 3, 4, 5 (Arity 5)
     * final RowData sourceRow = GenericRowData.of(1, 2, 3, 4, 5);
     *
     * // 2, 3 (Arity 2)
     * final RowData subRow = new SubRowData(sourceRow, 1, 3);
     * }</pre>
     */
    public SubRowData(RowData rowData, int startIndexInclusive, int endIndexExclusive) {
        this.rowData = rowData;
        this.startIndex = startIndexInclusive;
        this.endIndex = endIndexExclusive;

        if (startIndexInclusive < 0 || startIndexInclusive >= rowData.getArity()) {
            throw new IllegalArgumentException("startIndex must be within bounds.");
        }

        if (endIndexExclusive < 0
                || endIndexExclusive > rowData.getArity()
                || endIndexExclusive < startIndexInclusive) {
            throw new IllegalArgumentException("endIndex must be within bounds.");
        }
    }

    @Override
    public int getArity() {
        return endIndex - startIndex;
    }

    @Override
    public RowKind getRowKind() {
        return rowData.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        rowData.setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int pos) {
        return rowData.isNullAt(convertPos(pos));
    }

    @Override
    public boolean getBoolean(int pos) {
        return rowData.getBoolean(convertPos(pos));
    }

    @Override
    public byte getByte(int pos) {
        return rowData.getByte(convertPos(pos));
    }

    @Override
    public short getShort(int pos) {
        return rowData.getShort(convertPos(pos));
    }

    @Override
    public int getInt(int pos) {
        return rowData.getInt(convertPos(pos));
    }

    @Override
    public long getLong(int pos) {
        return rowData.getLong(convertPos(pos));
    }

    @Override
    public float getFloat(int pos) {
        return rowData.getFloat(convertPos(pos));
    }

    @Override
    public double getDouble(int pos) {
        return rowData.getDouble(convertPos(pos));
    }

    @Override
    public StringData getString(int pos) {
        return rowData.getString(convertPos(pos));
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return rowData.getDecimal(convertPos(pos), precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return rowData.getTimestamp(convertPos(pos), precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return rowData.getRawValue(convertPos(pos));
    }

    @Override
    public byte[] getBinary(int pos) {
        return rowData.getBinary(convertPos(pos));
    }

    @Override
    public ArrayData getArray(int pos) {
        return rowData.getArray(convertPos(pos));
    }

    @Override
    public MapData getMap(int pos) {
        return rowData.getMap(convertPos(pos));
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return rowData.getRow(convertPos(pos), numFields);
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof RowData)) {
            return false;
        }

        final SubRowData that = (SubRowData) other;
        return startIndex == that.startIndex
                && endIndex == that.endIndex
                && Objects.equals(rowData, that.rowData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowData, startIndex, endIndex);
    }

    // ---------------------------------------------------------------------------------------------

    private int convertPos(int pos) {
        final int convertedPos = pos + startIndex;
        if (convertedPos >= endIndex) {
            throw new IllegalArgumentException(
                    String.format("Position '%d' is out of bounds.", pos));
        }

        return convertedPos;
    }
}
