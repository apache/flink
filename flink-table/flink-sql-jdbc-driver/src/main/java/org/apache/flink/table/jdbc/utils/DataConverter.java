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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Timestamp;
import java.util.Map;

/** Convert data from row data for result set. */
public interface DataConverter {

    /** Returns the boolean value at the given position. */
    boolean getBoolean(RowData rowData, int pos);

    /** Returns the byte value at the given position. */
    byte getByte(RowData rowData, int pos);

    /** Returns the short value at the given position. */
    short getShort(RowData rowData, int pos);

    /** Returns the integer value at the given position. */
    int getInt(RowData rowData, int pos);

    /** Returns the long value at the given position. */
    long getLong(RowData rowData, int pos);

    /** Returns the float value at the given position. */
    float getFloat(RowData rowData, int pos);

    /** Returns the double value at the given position. */
    double getDouble(RowData rowData, int pos);

    /** Returns the string value at the given position. */
    String getString(RowData rowData, int pos);

    /**
     * Returns the decimal value at the given position.
     *
     * <p>The precision and scale are required to determine whether the decimal value was stored in
     * a compact representation (see {@link DecimalData}).
     */
    BigDecimal getDecimal(RowData rowData, int pos, int precision, int scale);

    /**
     * Returns the timestamp value at the given position.
     *
     * <p>The precision is required to determine whether the timestamp value was stored in a compact
     * representation (see {@link TimestampData}).
     */
    Timestamp getTimestamp(RowData rowData, int pos, int precision);

    /** Returns the binary value at the given position. */
    byte[] getBinary(RowData rowData, int pos);

    /** Returns the array value at the given position. */
    Array getArray(RowData rowData, int pos);

    /** Returns the map value at the given position. */
    Map<?, ?> getMap(RowData rowData, int pos);

    /**
     * Returns the row value at the given position.
     *
     * <p>The number of fields is required to correctly extract the row.
     */
    RowData getRow(RowData rowData, int pos, int numFields);
}
