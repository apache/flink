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

package org.apache.flink.table.data.binary;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * [4 byte(keyArray size in bytes)] + [Key BinaryArray] + [Value BinaryArray].
 *
 * <p>{@code BinaryMap} are influenced by Apache Spark UnsafeMapData.
 */
@Internal
public final class BinaryMapData extends BinarySection implements MapData {

    private final BinaryArrayData keys;
    private final BinaryArrayData values;

    public BinaryMapData() {
        keys = new BinaryArrayData();
        values = new BinaryArrayData();
    }

    public int size() {
        return keys.size();
    }

    @Override
    public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
        // Read the numBytes of key array from the first 4 bytes.
        final int keyArrayBytes = BinarySegmentUtils.getInt(segments, offset);
        assert keyArrayBytes >= 0 : "keyArraySize (" + keyArrayBytes + ") should >= 0";
        final int valueArrayBytes = sizeInBytes - keyArrayBytes - 4;
        assert valueArrayBytes >= 0 : "valueArraySize (" + valueArrayBytes + ") should >= 0";

        keys.pointTo(segments, offset + 4, keyArrayBytes);
        values.pointTo(segments, offset + 4 + keyArrayBytes, valueArrayBytes);

        assert keys.size() == values.size();

        this.segments = segments;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
    }

    public BinaryArrayData keyArray() {
        return keys;
    }

    public BinaryArrayData valueArray() {
        return values;
    }

    public Map<?, ?> toJavaMap(LogicalType keyType, LogicalType valueType) {
        Object[] keyArray = keys.toObjectArray(keyType);
        Object[] valueArray = values.toObjectArray(valueType);

        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < keyArray.length; i++) {
            map.put(keyArray[i], valueArray[i]);
        }
        return map;
    }

    public BinaryMapData copy() {
        return copy(new BinaryMapData());
    }

    public BinaryMapData copy(BinaryMapData reuse) {
        byte[] bytes = BinarySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
        reuse.pointTo(MemorySegmentFactory.wrap(bytes), 0, sizeInBytes);
        return reuse;
    }

    @Override
    public int hashCode() {
        return BinarySegmentUtils.hashByWords(segments, offset, sizeInBytes);
    }

    // ------------------------------------------------------------------------------------------
    // Construction Utilities
    // ------------------------------------------------------------------------------------------

    public static BinaryMapData valueOf(BinaryArrayData key, BinaryArrayData value) {
        checkArgument(key.segments.length == 1 && value.getSegments().length == 1);
        byte[] bytes = new byte[4 + key.sizeInBytes + value.sizeInBytes];
        MemorySegment segment = MemorySegmentFactory.wrap(bytes);
        segment.putInt(0, key.sizeInBytes);
        key.getSegments()[0].copyTo(key.getOffset(), segment, 4, key.sizeInBytes);
        value.getSegments()[0].copyTo(
                value.getOffset(), segment, 4 + key.sizeInBytes, value.sizeInBytes);
        BinaryMapData map = new BinaryMapData();
        map.pointTo(segment, 0, bytes.length);
        return map;
    }
}
