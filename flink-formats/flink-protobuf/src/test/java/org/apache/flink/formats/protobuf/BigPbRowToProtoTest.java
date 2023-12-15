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

import org.apache.flink.formats.protobuf.serialize.PbRowDataSerializationSchema;
import org.apache.flink.formats.protobuf.testproto.BigPbClass;
import org.apache.flink.formats.protobuf.util.PbToRowTypeUtil;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for huge proto definition, which may trigger some special optimizations such as code
 * splitting.
 */
public class BigPbRowToProtoTest {

    @Test
    public void testSimple() throws Exception {
        GenericRowData rowData = new GenericRowData(33);
        rowData.setField(0, 20);
        rowData.setField(1, false);
        rowData.setField(2, StringData.fromString("test1"));
        rowData.setField(3, new byte[] {1, 2, 3});
        rowData.setField(4, 2.5);
        rowData.setField(5, 1.5F);
        rowData.setField(6, 3);
        rowData.setField(7, 7L);
        rowData.setField(8, 9L);
        rowData.setField(9, new byte[] {4, 5, 6});
        rowData.setField(10, 6.5);
        rowData.setField(11, new byte[] {7, 8, 9});
        rowData.setField(12, true);
        rowData.setField(13, StringData.fromString("test2"));
        rowData.setField(14, 3.5F);
        rowData.setField(15, 8);
        rowData.setField(16, new byte[] {10, 11, 12});
        rowData.setField(17, true);
        rowData.setField(18, StringData.fromString("test3"));
        rowData.setField(19, 4.5F);
        rowData.setField(20, 1);
        rowData.setField(21, 2L);
        rowData.setField(22, 3);
        rowData.setField(23, 4L);
        rowData.setField(24, 5.5);
        rowData.setField(25, 6);
        rowData.setField(26, 7L);
        rowData.setField(27, true);
        rowData.setField(
                28,
                new GenericArrayData(
                        new StringData[] {
                            StringData.fromString("value1"),
                            StringData.fromString("value2"),
                            StringData.fromString("value3")
                        }));
        rowData.setField(29, 3.5F);
        rowData.setField(30, StringData.fromString("test4"));
        rowData.setField(31, null);
        Map<StringData, StringData> map1 =
                new HashMap() {
                    {
                        put(StringData.fromString("key1"), StringData.fromString("value1"));
                        put(StringData.fromString("key2"), StringData.fromString("value2"));
                        put(StringData.fromString("key3"), StringData.fromString("value3"));
                    }
                };
        rowData.setField(32, new GenericMapData(map1));

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(rowData, BigPbClass.BigPbMessage.class);

        BigPbClass.BigPbMessage bigPbMessage = BigPbClass.BigPbMessage.parseFrom(bytes);
        assertEquals(rowData.getField(0), bigPbMessage.getIntField1());
        assertEquals(rowData.getField(1), bigPbMessage.getBoolField2());
        assertEquals(rowData.getField(2).toString(), bigPbMessage.getStringField3());
        assertArrayEquals(
                ((byte[]) rowData.getField(3)), bigPbMessage.getBytesField4().toByteArray());
        assertEquals(rowData.getField(4), bigPbMessage.getDoubleField5());
        assertEquals(rowData.getField(5), bigPbMessage.getFloatField6());
        assertEquals(rowData.getField(6), bigPbMessage.getUint32Field7());
        assertEquals(rowData.getField(7), bigPbMessage.getInt64Field8());
        assertEquals(rowData.getField(8), bigPbMessage.getUint64Field9());
        assertArrayEquals(
                ((byte[]) rowData.getField(9)), bigPbMessage.getBytesField10().toByteArray());
        assertEquals(rowData.getField(10), bigPbMessage.getDoubleField11());
        assertArrayEquals(
                ((byte[]) rowData.getField(11)), bigPbMessage.getBytesField12().toByteArray());
        assertEquals(rowData.getField(12), bigPbMessage.getBoolField13());
        assertEquals(rowData.getField(13).toString(), bigPbMessage.getStringField14());
        assertEquals(rowData.getField(14), bigPbMessage.getFloatField15());
        assertEquals(rowData.getField(15), bigPbMessage.getInt32Field16());
        assertArrayEquals(
                ((byte[]) rowData.getField(16)), bigPbMessage.getBytesField17().toByteArray());
        assertEquals(rowData.getField(17), bigPbMessage.getBoolField18());
        assertEquals(rowData.getField(18).toString(), bigPbMessage.getStringField19());
        assertEquals(rowData.getField(19), bigPbMessage.getFloatField20());
        assertEquals(rowData.getField(20), bigPbMessage.getFixed32Field21());
        assertEquals(rowData.getField(21), bigPbMessage.getFixed64Field22());
        assertEquals(rowData.getField(22), bigPbMessage.getSfixed32Field23());
        assertEquals(rowData.getField(23), bigPbMessage.getSfixed64Field24());
        assertEquals(rowData.getField(24), bigPbMessage.getDoubleField25());
        assertEquals(rowData.getField(25), bigPbMessage.getUint32Field26());
        assertEquals(rowData.getField(26), bigPbMessage.getUint64Field27());
        assertEquals(rowData.getField(27), bigPbMessage.getBoolField28());
        assertEquals(
                ((GenericArrayData) rowData.getField(28)).getString(0).toString(),
                bigPbMessage.getField29List().get(0));
        assertEquals(
                ((GenericArrayData) rowData.getField(28)).getString(1).toString(),
                bigPbMessage.getField29List().get(1));
        assertEquals(
                ((GenericArrayData) rowData.getField(28)).getString(2).toString(),
                bigPbMessage.getField29List().get(2));
        assertEquals(rowData.getField(29), bigPbMessage.getFloatField30());
        assertEquals(rowData.getField(30).toString(), bigPbMessage.getStringField31());

        ArrayData keySet = rowData.getMap(32).keyArray();
        ArrayData valueSet = rowData.getMap(32).valueArray();
        assertEquals(keySet.getString(0).toString(), "key2");
        assertEquals(keySet.getString(1).toString(), "key3");
        assertEquals(keySet.getString(2).toString(), "key1");
        assertEquals(valueSet.getString(0).toString(), "value2");
        assertEquals(valueSet.getString(1).toString(), "value3");
        assertEquals(valueSet.getString(2).toString(), "value1");
    }

    @Test
    public void testSplitInSerialization() throws Exception {
        RowType rowType = PbToRowTypeUtil.generateRowType(BigPbClass.BigPbMessage.getDescriptor());
        PbFormatConfig formatConfig =
                new PbFormatConfig(BigPbClass.BigPbMessage.class.getName(), false, false, "");
        PbRowDataSerializationSchema pbRowDataSerializationSchema =
                new PbRowDataSerializationSchema(rowType, formatConfig);
        pbRowDataSerializationSchema.open(null);
        // make sure code is split
        assertTrue(pbRowDataSerializationSchema.isCodeSplit());
    }
}
