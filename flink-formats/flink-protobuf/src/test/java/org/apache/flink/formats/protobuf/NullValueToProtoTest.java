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

package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.testproto.NullTest;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import com.google.protobuf.ByteString;
import org.junit.Test;

import static org.apache.flink.formats.protobuf.ProtobufTestHelper.mapOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test conversion of null values from flink internal data to proto data. Proto data does not permit
 * null values in array/map data.
 */
public class NullValueToProtoTest {
    @Test
    public void testSimple() throws Exception {
        RowData row =
                GenericRowData.of(
                        // string
                        new GenericMapData(
                                mapOf(
                                        StringData.fromString("key"),
                                        null,
                                        StringData.fromString(""),
                                        StringData.fromString("value"))),
                        // int32
                        new GenericMapData(mapOf(0, 1, 1, null)),
                        // int64
                        new GenericMapData(mapOf(0L, 1L, 1L, null)),
                        // boolean
                        new GenericMapData(mapOf(false, true, true, null)),
                        // float
                        new GenericMapData(mapOf(StringData.fromString("key"), null)),
                        // double
                        new GenericMapData(mapOf(StringData.fromString("key"), null)),
                        // enum
                        new GenericMapData(mapOf(StringData.fromString("key"), null)),
                        // message
                        new GenericMapData(mapOf(StringData.fromString("key"), null)),
                        // bytes
                        new GenericMapData(mapOf(StringData.fromString("key"), null)),
                        // string
                        new GenericArrayData(new Object[] {null}),
                        // int
                        new GenericArrayData(new Object[] {null}),
                        // long
                        new GenericArrayData(new Object[] {null}),
                        // boolean
                        new GenericArrayData(new Object[] {null}),
                        // float
                        new GenericArrayData(new Object[] {null}),
                        // double
                        new GenericArrayData(new Object[] {null}),
                        // enum
                        new GenericArrayData(new Object[] {null}),
                        // message, cannot be null
                        new GenericArrayData(new Object[] {null}),
                        // bytes, cannot be null
                        new GenericArrayData(new Object[] {null}));
        byte[] bytes =
                ProtobufTestHelper.rowToPbBytes(
                        row,
                        NullTest.class,
                        new PbFormatConfig(NullTest.class.getName(), false, false, ""),
                        false);
        NullTest nullTest = NullTest.parseFrom(bytes);
        // string map
        assertEquals(2, nullTest.getStringMapCount());
        assertTrue(nullTest.getStringMapMap().containsKey(""));
        assertTrue(nullTest.getStringMapMap().containsKey("key"));
        assertEquals("value", nullTest.getStringMapMap().get(""));
        assertEquals("", nullTest.getStringMapMap().get("key"));
        // int32 map
        assertEquals(2, nullTest.getIntMapCount());
        assertTrue(nullTest.getIntMapMap().containsKey(0));
        assertTrue(nullTest.getIntMapMap().containsKey(1));
        assertEquals(Integer.valueOf(1), nullTest.getIntMapMap().get(0));
        assertEquals(Integer.valueOf(0), nullTest.getIntMapMap().get(1));
        // int64 map
        assertEquals(2, nullTest.getIntMapCount());
        assertTrue(nullTest.getLongMapMap().containsKey(0L));
        assertTrue(nullTest.getLongMapMap().containsKey(1L));
        assertEquals(Long.valueOf(1L), nullTest.getLongMapMap().get(0L));
        assertEquals(Long.valueOf(0L), nullTest.getLongMapMap().get(1L));
        // bool map
        assertEquals(2, nullTest.getBooleanMapCount());
        assertTrue(nullTest.getBooleanMapMap().containsKey(false));
        assertTrue(nullTest.getBooleanMapMap().containsKey(true));
        assertEquals(Boolean.TRUE, nullTest.getBooleanMapMap().get(false));
        assertEquals(Boolean.FALSE, nullTest.getBooleanMapMap().get(true));
        // float map
        assertEquals(1, nullTest.getFloatMapCount());
        assertEquals(Float.valueOf(0.0f), nullTest.getFloatMapMap().get("key"));
        // double map
        assertEquals(1, nullTest.getDoubleMapCount());
        assertEquals(Double.valueOf(0.0), nullTest.getDoubleMapMap().get("key"));
        // enum map
        assertEquals(1, nullTest.getEnumMapCount());
        assertEquals(NullTest.Corpus.UNIVERSAL, nullTest.getEnumMapMap().get("key"));
        // message map
        assertEquals(1, nullTest.getMessageMapCount());
        assertEquals(
                NullTest.InnerMessageTest.getDefaultInstance(),
                nullTest.getMessageMapMap().get("key"));
        // bytes map
        assertEquals(1, nullTest.getBytesMapCount());
        assertEquals(ByteString.EMPTY, nullTest.getBytesMapMap().get("key"));

        // string array
        assertEquals(1, nullTest.getStringArrayCount());
        assertEquals("", nullTest.getStringArrayList().get(0));
        // int array
        assertEquals(1, nullTest.getIntArrayCount());
        assertEquals(Integer.valueOf(0), nullTest.getIntArrayList().get(0));
        // long array
        assertEquals(1, nullTest.getLongArrayCount());
        assertEquals(Long.valueOf(0L), nullTest.getLongArrayList().get(0));
        // float array
        assertEquals(1, nullTest.getFloatArrayCount());
        assertEquals(Float.valueOf(0), nullTest.getFloatArrayList().get(0));
        // double array
        assertEquals(1, nullTest.getDoubleArrayCount());
        assertEquals(Double.valueOf(0), nullTest.getDoubleArrayList().get(0));
        // boolean array
        assertEquals(1, nullTest.getBooleanArrayCount());
        assertEquals(Boolean.FALSE, nullTest.getBooleanArrayList().get(0));
        // enum array
        assertEquals(1, nullTest.getEnumArrayCount());
        assertEquals(NullTest.Corpus.UNIVERSAL, nullTest.getEnumArrayList().get(0));
        // message array
        assertEquals(1, nullTest.getMessageArrayCount());
        assertEquals(
                NullTest.InnerMessageTest.getDefaultInstance(),
                nullTest.getMessageArrayList().get(0));
        // bytes array
        assertEquals(1, nullTest.getBytesArrayCount());
        assertEquals(ByteString.EMPTY, nullTest.getBytesArrayList().get(0));
    }

    @Test
    public void testNullStringLiteral() throws Exception {
        RowData row =
                GenericRowData.of(
                        // string
                        new GenericMapData(
                                mapOf(
                                        StringData.fromString("key"),
                                        null,
                                        null,
                                        StringData.fromString("value"))),
                        // int32
                        null,
                        // int64
                        null,
                        // boolean
                        null,
                        // float
                        null,
                        // double
                        null,
                        // enum
                        null,
                        // message
                        null,
                        // bytes
                        null,
                        // string
                        null,
                        // int
                        null,
                        // long
                        null,
                        // boolean
                        null,
                        // float
                        null,
                        // double
                        null,
                        // enum
                        null,
                        // message, cannot be null
                        null,
                        // bytes, cannot be null
                        null);
        byte[] bytes =
                ProtobufTestHelper.rowToPbBytes(
                        row,
                        NullTest.class,
                        new PbFormatConfig(NullTest.class.getName(), false, false, "NULL"),
                        false);
        NullTest nullTest = NullTest.parseFrom(bytes);
        assertEquals("NULL", nullTest.getStringMapMap().get("key"));
    }
}
