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

package org.apache.flink.connector.file.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import java.util.List;
import java.util.Objects;

/**
 * An implementation of {@link RowData} which is backed by two {@link RowData} with a well-defined
 * index mapping, One of the rows is fixed, while the other can be swapped for performant changes in
 * hot code paths. The {@link RowKind} is inherited from the mutable row.
 */
@Internal
public class EnrichedRowData implements RowData {

    private final RowData fixedRow;
    // The index mapping is built as follows: positive indexes are indexes refer to mutable row
    // positions,
    // while negative indexes (with -1 offset) refer to fixed row positions.
    // For example an index mapping [0, 1, -1, -2, 2] means:
    // * Index 0 -> mutable row index 0
    // * Index 1 -> mutable row index 1
    // * Index -1 -> fixed row index 0
    // * Index -2 -> fixed row index 1
    // * Index 2 -> mutable row index 2
    private final int[] indexMapping;

    private RowData mutableRow;

    public EnrichedRowData(RowData fixedRow, int[] indexMapping) {
        this.fixedRow = fixedRow;
        this.indexMapping = indexMapping;
    }

    /**
     * Replaces the mutable {@link RowData} backing this {@link EnrichedRowData}.
     *
     * <p>This method replaces the mutable row data in place and does not return a new object. This
     * is done for performance reasons.
     */
    public EnrichedRowData replaceMutableRow(RowData mutableRow) {
        this.mutableRow = mutableRow;
        return this;
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public int getArity() {
        return indexMapping.length;
    }

    @Override
    public RowKind getRowKind() {
        return mutableRow.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        mutableRow.setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int pos) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.isNullAt(index);
        } else {
            return fixedRow.isNullAt(-(index + 1));
        }
    }

    @Override
    public boolean getBoolean(int pos) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getBoolean(index);
        } else {
            return fixedRow.getBoolean(-(index + 1));
        }
    }

    @Override
    public byte getByte(int pos) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getByte(index);
        } else {
            return fixedRow.getByte(-(index + 1));
        }
    }

    @Override
    public short getShort(int pos) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getShort(index);
        } else {
            return fixedRow.getShort(-(index + 1));
        }
    }

    @Override
    public int getInt(int pos) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getInt(index);
        } else {
            return fixedRow.getInt(-(index + 1));
        }
    }

    @Override
    public long getLong(int pos) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getLong(index);
        } else {
            return fixedRow.getLong(-(index + 1));
        }
    }

    @Override
    public float getFloat(int pos) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getFloat(index);
        } else {
            return fixedRow.getFloat(-(index + 1));
        }
    }

    @Override
    public double getDouble(int pos) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getDouble(index);
        } else {
            return fixedRow.getDouble(-(index + 1));
        }
    }

    @Override
    public StringData getString(int pos) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getString(index);
        } else {
            return fixedRow.getString(-(index + 1));
        }
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getDecimal(index, precision, scale);
        } else {
            return fixedRow.getDecimal(-(index + 1), precision, scale);
        }
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getTimestamp(index, precision);
        } else {
            return fixedRow.getTimestamp(-(index + 1), precision);
        }
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getRawValue(index);
        } else {
            return fixedRow.getRawValue(-(index + 1));
        }
    }

    @Override
    public byte[] getBinary(int pos) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getBinary(index);
        } else {
            return fixedRow.getBinary(-(index + 1));
        }
    }

    @Override
    public ArrayData getArray(int pos) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getArray(index);
        } else {
            return fixedRow.getArray(-(index + 1));
        }
    }

    @Override
    public MapData getMap(int pos) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getMap(index);
        } else {
            return fixedRow.getMap(-(index + 1));
        }
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        int index = indexMapping[pos];
        if (index >= 0) {
            return mutableRow.getRow(index, numFields);
        } else {
            return fixedRow.getRow(-(index + 1), numFields);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EnrichedRowData that = (EnrichedRowData) o;
        return Objects.equals(this.fixedRow, that.fixedRow)
                && Objects.equals(this.mutableRow, that.mutableRow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fixedRow, mutableRow);
    }

    @Override
    public String toString() {
        return mutableRow.getRowKind().shortString()
                + "{"
                + "fixedRow="
                + fixedRow
                + ", mutableRow="
                + mutableRow
                + '}';
    }

    /**
     * Creates a new {@link EnrichedRowData} with the provided {@code fixedRow} as the immutable
     * static row, and uses the {@code producedRowFields}, {@code fixedRowFields} and {@code
     * mutableRowFields} arguments to compute the indexes mapping.
     *
     * <p>The {@code producedRowFields} should include the name of fields of the full row once
     * mutable and fixed rows are merged, while {@code fixedRowFields} and {@code mutableRowFields}
     * should contain respectively the field names of fixed row and mutable row. All the lists are
     * ordered with indexes matching the position of the field in the row. As an example, for a
     * complete row {@code (a, b, c)} the mutable row might be {@code (a, c)} and the fixed row
     * might be {@code (b)}
     */
    public static EnrichedRowData from(
            RowData fixedRow,
            List<String> producedRowFields,
            List<String> mutableRowFields,
            List<String> fixedRowFields) {
        return new EnrichedRowData(
                fixedRow, computeIndexMapping(producedRowFields, mutableRowFields, fixedRowFields));
    }

    /**
     * This method computes the index mapping for {@link EnrichedRowData}.
     *
     * @see EnrichedRowData#from(RowData, List, List, List)
     */
    public static int[] computeIndexMapping(
            List<String> producedRowFields,
            List<String> mutableRowFields,
            List<String> fixedRowFields) {
        int[] indexMapping = new int[producedRowFields.size()];

        for (int i = 0; i < producedRowFields.size(); i++) {
            String fieldName = producedRowFields.get(i);

            int newIndex = mutableRowFields.indexOf(fieldName);
            if (newIndex < 0) {
                newIndex = -(fixedRowFields.indexOf(fieldName) + 1);
            }

            indexMapping[i] = newIndex;
        }

        return indexMapping;
    }
}
