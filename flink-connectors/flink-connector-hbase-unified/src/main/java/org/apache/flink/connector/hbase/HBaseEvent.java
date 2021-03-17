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

package org.apache.flink.connector.hbase;

import org.apache.flink.connector.hbase.sink.HBaseSinkSerializer;
import org.apache.flink.connector.hbase.source.HBaseSource;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceEvent;

import org.apache.hadoop.hbase.Cell;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * The base HBaseEvent which needs to be created in the {@link HBaseSinkSerializer} to write data to
 * HBase. The subclass {@link HBaseSourceEvent} contains additional information and is used by the
 * {@link HBaseSource} to represent an incoming event from HBase.
 */
public class HBaseEvent {

    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private final String rowId;
    private final String columnFamily;
    private final String qualifier;
    private final byte[] payload;
    private final Cell.Type type;

    protected HBaseEvent(
            Cell.Type type, String rowId, String columnFamily, String qualifier, byte[] payload) {
        this.rowId = rowId;
        this.columnFamily = columnFamily;
        this.qualifier = qualifier;
        this.payload = payload;
        this.type = type;
    }

    public static HBaseEvent deleteWith(String rowId, String columnFamily, String qualifier) {
        return new HBaseEvent(Cell.Type.Delete, rowId, columnFamily, qualifier, null);
    }

    public static HBaseEvent putWith(
            String rowId, String columnFamily, String qualifier, byte[] payload) {
        return new HBaseEvent(Cell.Type.Put, rowId, columnFamily, qualifier, payload);
    }

    @Override
    public String toString() {
        return type.name()
                + " "
                + rowId
                + " "
                + columnFamily
                + " "
                + qualifier
                + " "
                + new String(payload);
    }

    public Cell.Type getType() {
        return type;
    }

    public byte[] getPayload() {
        return payload;
    }

    public String getRowId() {
        return rowId;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public String getQualifier() {
        return qualifier;
    }

    public byte[] getRowIdBytes() {
        return rowId.getBytes(DEFAULT_CHARSET);
    }

    public byte[] getColumnFamilyBytes() {
        return columnFamily.getBytes(DEFAULT_CHARSET);
    }

    public byte[] getQualifierBytes() {
        return qualifier.getBytes(DEFAULT_CHARSET);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HBaseEvent that = (HBaseEvent) o;
        return rowId.equals(that.rowId)
                && columnFamily.equals(that.columnFamily)
                && qualifier.equals(that.qualifier)
                && Arrays.equals(payload, that.payload)
                && type == that.type;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(rowId, columnFamily, qualifier, type);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
}
