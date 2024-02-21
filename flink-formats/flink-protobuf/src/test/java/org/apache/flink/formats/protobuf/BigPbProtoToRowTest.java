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

import org.apache.flink.formats.protobuf.deserialize.PbRowDataDeserializationSchema;
import org.apache.flink.formats.protobuf.testproto.BigPbClass;
import org.apache.flink.formats.protobuf.util.PbToRowTypeUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.ByteString;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Test for huge proto definition, which may trigger some special optimizations such as code
 * splitting.
 */
public class BigPbProtoToRowTest {

    @Test
    public void testSimple() throws Exception {
        BigPbClass.BigPbMessage bigPbMessage =
                BigPbClass.BigPbMessage.newBuilder()
                        .setIntField1(5)
                        .setBoolField2(false)
                        .setStringField3("test1")
                        .setBytesField4(ByteString.copyFrom(new byte[] {1, 2, 3}))
                        .setDoubleField5(2.5)
                        .setFloatField6(1.5F)
                        .setUint32Field7(3)
                        .setInt64Field8(7L)
                        .setUint64Field9(9L)
                        .setBytesField10(ByteString.copyFrom(new byte[] {4, 5, 6}))
                        .setDoubleField11(6.5)
                        .setBytesField12(ByteString.copyFrom(new byte[] {7, 8, 9}))
                        .setBoolField13(true)
                        .setStringField14("test2")
                        .setFloatField15(3.5F)
                        .setInt32Field16(8)
                        .setBytesField17(ByteString.copyFrom(new byte[] {10, 11, 12}))
                        .setBoolField18(true)
                        .setStringField19("test3")
                        .setFloatField20(4.5F)
                        .setFixed32Field21(1)
                        .setFixed64Field22(2L)
                        .setSfixed32Field23(3)
                        .setSfixed64Field24(4L)
                        .setDoubleField25(5.5)
                        .setUint32Field26(6)
                        .setUint64Field27(7L)
                        .setBoolField28(true)
                        .addField29("value1")
                        .addField29("value2")
                        .addField29("value3")
                        .setFloatField30(8.5F)
                        .setStringField31("test4")
                        .putMapField32("key1", ByteString.copyFrom(new byte[] {13, 14, 15}))
                        .putMapField32("key2", ByteString.copyFrom(new byte[] {16, 17, 18}))
                        .putMapField33("key1", "value1")
                        .putMapField33("key2", "value2")
                        .build();

        RowData row =
                ProtobufTestHelper.pbBytesToRow(
                        BigPbClass.BigPbMessage.class, bigPbMessage.toByteArray());

        assertEquals(5, row.getInt(0));
        assertFalse(row.getBoolean(1));
        assertEquals("test1", row.getString(2).toString());
        assertArrayEquals(new byte[] {1, 2, 3}, row.getBinary(3));
        assertEquals(2.5, row.getDouble(4), 0.0);
        assertEquals(1.5F, row.getFloat(5), 0.0);
        assertEquals(3, row.getInt(6));
        assertEquals(7L, row.getLong(7));
        assertEquals(9L, row.getLong(8));
        assertArrayEquals(new byte[] {4, 5, 6}, row.getBinary(9));
        assertEquals(6.5, row.getDouble(10), 0.0);
        assertArrayEquals(new byte[] {7, 8, 9}, row.getBinary(11));
        assertTrue(row.getBoolean(12));
        assertEquals("test2", row.getString(13).toString());
        assertEquals(3.5F, row.getFloat(14), 0.0);
        assertEquals(8, row.getInt(15));
        assertArrayEquals(new byte[] {10, 11, 12}, row.getBinary(16));
        assertTrue(row.getBoolean(17));
        assertEquals("test3", row.getString(18).toString());
        assertEquals(4.5F, row.getFloat(19), 0.0);
        assertEquals(1, row.getInt(20));
        assertEquals(2L, row.getLong(21));
        assertEquals(3, row.getInt(22));
        assertEquals(4L, row.getLong(23));
        assertEquals(5.5, row.getDouble(24), 0.0);
        assertEquals(6, row.getInt(25));
        assertEquals(7L, row.getLong(26));
        assertTrue(row.getBoolean(27));
        assertEquals("value1", row.getArray(28).getString(0).toString());
        assertEquals("value2", row.getArray(28).getString(1).toString());
        assertEquals("value3", row.getArray(28).getString(2).toString());
        assertEquals(8.5F, row.getFloat(29), 0.0);
        assertEquals("test4", row.getString(30).toString());
        assertArrayEquals(new byte[] {13, 14, 15}, row.getMap(31).valueArray().getBinary(0));
        assertArrayEquals(new byte[] {16, 17, 18}, row.getMap(31).valueArray().getBinary(1));
        assertEquals("value1", row.getMap(32).valueArray().getString(0).toString());
        assertEquals("value2", row.getMap(32).valueArray().getString(1).toString());
    }

    @Test
    public void testSplitInDeserialization() throws Exception {
        RowType rowType = PbToRowTypeUtil.generateRowType(BigPbClass.BigPbMessage.getDescriptor());
        PbFormatConfig formatConfig =
                new PbFormatConfig(BigPbClass.BigPbMessage.class.getName(), false, false, "");
        PbRowDataDeserializationSchema pbRowDataDeserializationSchema =
                new PbRowDataDeserializationSchema(
                        rowType, InternalTypeInfo.of(rowType), formatConfig);
        pbRowDataDeserializationSchema.open(null);
        // make sure code is split
        assertTrue(pbRowDataDeserializationSchema.isCodeSplit());
    }
}
