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
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Arrays;

/**
 * An implementation of {@link RowData} which provides a projected view of the underlying {@link
 * RowData}.
 *
 * <p>Projection includes both reducing the accessible fields and reordering them.
 *
 * <p>Note: This class supports only top-level projections, not nested projections.
 */
@PublicEvolving
public class ProjectedRowData implements RowData {

    private final int[] indexMapping;

    private RowData row;

    private ProjectedRowData(int[] indexMapping) {
        this.indexMapping = indexMapping;
    }

    /**
     * Replaces the underlying {@link RowData} backing this {@link ProjectedRowData}.
     *
     * <p>This method replaces the row data in place and does not return a new object. This is done
     * for performance reasons.
     */
    public ProjectedRowData replaceRow(RowData row) {
        this.row = row;
        return this;
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public int getArity() {
        return indexMapping.length;
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
        return row.isNullAt(indexMapping[pos]);
    }

    @Override
    public boolean getBoolean(int pos) {
        return row.getBoolean(indexMapping[pos]);
    }

    @Override
    public byte getByte(int pos) {
        return row.getByte(indexMapping[pos]);
    }

    @Override
    public short getShort(int pos) {
        return row.getShort(indexMapping[pos]);
    }

    @Override
    public int getInt(int pos) {
        return row.getInt(indexMapping[pos]);
    }

    @Override
    public long getLong(int pos) {
        return row.getLong(indexMapping[pos]);
    }

    @Override
    public float getFloat(int pos) {
        return row.getFloat(indexMapping[pos]);
    }

    @Override
    public double getDouble(int pos) {
        return row.getDouble(indexMapping[pos]);
    }

    @Override
    public StringData getString(int pos) {
        return row.getString(indexMapping[pos]);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return row.getDecimal(indexMapping[pos], precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return row.getTimestamp(indexMapping[pos], precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return row.getRawValue(indexMapping[pos]);
    }

    @Override
    public byte[] getBinary(int pos) {
        return row.getBinary(indexMapping[pos]);
    }

    @Override
    public ArrayData getArray(int pos) {
        return row.getArray(indexMapping[pos]);
    }

    @Override
    public MapData getMap(int pos) {
        return row.getMap(indexMapping[pos]);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return row.getRow(indexMapping[pos], numFields);
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException("Projected row data cannot be compared");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("Projected row data cannot be hashed");
    }

    @Override
    public String toString() {
        return row.getRowKind().shortString()
                + "{"
                + "indexMapping="
                + Arrays.toString(indexMapping)
                + ", mutableRow="
                + row
                + '}';
    }

    /**
     * Like {@link #from(int[])}, but throws {@link IllegalArgumentException} if the provided {@code
     * projection} array contains nested projections, which are not supported by {@link
     * ProjectedRowData}.
     *
     * <p>The array represents the mapping of the fields of the original {@link DataType}, including
     * nested rows. For example, {@code [[0, 2, 1], ...]} specifies to include the 2nd field of the
     * 3rd field of the 1st field in the top-level row.
     *
     * @see Projection
     * @see ProjectedRowData
     */
    public static ProjectedRowData from(int[][] projection) throws IllegalArgumentException {
        return new ProjectedRowData(
                Arrays.stream(projection)
                        .mapToInt(
                                arr -> {
                                    if (arr.length != 1) {
                                        throw new IllegalArgumentException(
                                                "ProjectedRowData doesn't support nested projections");
                                    }
                                    return arr[0];
                                })
                        .toArray());
    }

    /**
     * Create an empty {@link ProjectedRowData} starting from a {@code projection} array.
     *
     * <p>The array represents the mapping of the fields of the original {@link DataType}. For
     * example, {@code [0, 2, 1]} specifies to include in the following order the 1st field, the 3rd
     * field and the 2nd field of the row.
     *
     * @see Projection
     * @see ProjectedRowData
     */
    public static ProjectedRowData from(int[] projection) {
        return new ProjectedRowData(projection);
    }

    /**
     * Create an empty {@link ProjectedRowData} starting from a {@link Projection}.
     *
     * <p>Throws {@link IllegalStateException} if the provided {@code projection} array contains
     * nested projections, which are not supported by {@link ProjectedRowData}.
     *
     * @see Projection
     * @see ProjectedRowData
     */
    public static ProjectedRowData from(Projection projection) {
        return new ProjectedRowData(projection.toTopLevelIndexes());
    }
}
