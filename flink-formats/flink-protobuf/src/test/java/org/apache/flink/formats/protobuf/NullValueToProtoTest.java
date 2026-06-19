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
import org.junit.jupiter.api.Test;

import static org.apache.flink.formats.protobuf.ProtobufTestHelper.mapOf;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test conversion of null values from flink internal data to proto data. Proto data does not permit
 * null values in array/map data.
 */
class NullValueToProtoTest {
    @Test
    void testSimple() throws Exception {
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
        assertThat(nullTest.getStringMapCount()).isEqualTo(2);
        assertThat(nullTest.getStringMapMap().containsKey("")).isTrue();
        assertThat(nullTest.getStringMapMap().containsKey("key")).isTrue();
        assertThat(nullTest.getStringMapMap().get("")).isEqualTo("value");
        assertThat(nullTest.getStringMapMap().get("key")).isEqualTo("");
        // int32 map
        assertThat(nullTest.getIntMapCount()).isEqualTo(2);
        assertThat(nullTest.getIntMapMap().containsKey(0)).isTrue();
        assertThat(nullTest.getIntMapMap().containsKey(1)).isTrue();
        assertThat(nullTest.getIntMapMap().get(0)).isEqualTo(Integer.valueOf(1));
        assertThat(nullTest.getIntMapMap().get(1)).isEqualTo(Integer.valueOf(0));
        // int64 map
        assertThat(nullTest.getIntMapCount()).isEqualTo(2);
        assertThat(nullTest.getLongMapMap().containsKey(0L)).isTrue();
        assertThat(nullTest.getLongMapMap().containsKey(1L)).isTrue();
        assertThat(nullTest.getLongMapMap().get(0L)).isEqualTo(Long.valueOf(1L));
        assertThat(nullTest.getLongMapMap().get(1L)).isEqualTo(Long.valueOf(0L));
        // bool map
        assertThat(nullTest.getBooleanMapCount()).isEqualTo(2);
        assertThat(nullTest.getBooleanMapMap().containsKey(false)).isTrue();
        assertThat(nullTest.getBooleanMapMap().containsKey(true)).isTrue();
        assertThat(nullTest.getBooleanMapMap().get(false)).isEqualTo(Boolean.TRUE);
        assertThat(nullTest.getBooleanMapMap().get(true)).isEqualTo(Boolean.FALSE);
        // float map
        assertThat(nullTest.getFloatMapCount()).isEqualTo(1);
        assertThat(nullTest.getFloatMapMap().get("key")).isEqualTo(Float.valueOf(0.0f));
        // double map
        assertThat(nullTest.getDoubleMapCount()).isEqualTo(1);
        assertThat(nullTest.getDoubleMapMap().get("key")).isEqualTo(Double.valueOf(0.0));
        // enum map
        assertThat(nullTest.getEnumMapCount()).isEqualTo(1);
        assertThat(nullTest.getEnumMapMap().get("key")).isEqualTo(NullTest.Corpus.UNIVERSAL);
        // message map
        assertThat(nullTest.getMessageMapCount()).isEqualTo(1);
        assertThat(nullTest.getMessageMapMap().get("key"))
                .isEqualTo(NullTest.InnerMessageTest.getDefaultInstance());
        // bytes map
        assertThat(nullTest.getBytesMapCount()).isEqualTo(1);
        assertThat(nullTest.getBytesMapMap().get("key")).isEqualTo(ByteString.EMPTY);

        // string array
        assertThat(nullTest.getStringArrayCount()).isEqualTo(1);
        assertThat(nullTest.getStringArrayList().get(0)).isEqualTo("");
        // int array
        assertThat(nullTest.getIntArrayCount()).isEqualTo(1);
        assertThat(nullTest.getIntArrayList().get(0)).isEqualTo(Integer.valueOf(0));
        // long array
        assertThat(nullTest.getLongArrayCount()).isEqualTo(1);
        assertThat(nullTest.getLongArrayList().get(0)).isEqualTo(Long.valueOf(0L));
        // float array
        assertThat(nullTest.getFloatArrayCount()).isEqualTo(1);
        assertThat(nullTest.getFloatArrayList().get(0)).isEqualTo(Float.valueOf(0));
        // double array
        assertThat(nullTest.getDoubleArrayCount()).isEqualTo(1);
        assertThat(nullTest.getDoubleArrayList().get(0)).isEqualTo(Double.valueOf(0));
        // boolean array
        assertThat(nullTest.getBooleanArrayCount()).isEqualTo(1);
        assertThat(nullTest.getBooleanArrayList().get(0)).isEqualTo(Boolean.FALSE);
        // enum array
        assertThat(nullTest.getEnumArrayCount()).isEqualTo(1);
        assertThat(nullTest.getEnumArrayList().get(0)).isEqualTo(NullTest.Corpus.UNIVERSAL);
        // message array
        assertThat(nullTest.getMessageArrayCount()).isEqualTo(1);
        assertThat(nullTest.getMessageArrayList().get(0))
                .isEqualTo(NullTest.InnerMessageTest.getDefaultInstance());
        // bytes array
        assertThat(nullTest.getBytesArrayCount()).isEqualTo(1);
        assertThat(nullTest.getBytesArrayList().get(0)).isEqualTo(ByteString.EMPTY);
    }

    @Test
    void testNullStringLiteral() throws Exception {
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
        assertThat(nullTest.getStringMapMap().get("key")).isEqualTo("NULL");
    }
}
