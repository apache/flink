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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for huge proto definition, which may trigger some special optimizations such as code
 * splitting.
 */
class BigPbRowToProtoTest {

    @Test
    void testSimple() throws Exception {
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
        assertThat(bigPbMessage.getIntField1()).isEqualTo(rowData.getField(0));
        assertThat(bigPbMessage.getBoolField2()).isEqualTo(rowData.getField(1));
        assertThat(bigPbMessage.getStringField3()).isEqualTo(rowData.getField(2).toString());
        assertThat(bigPbMessage.getBytesField4().toByteArray())
                .isEqualTo(((byte[]) rowData.getField(3)));
        assertThat(bigPbMessage.getDoubleField5()).isEqualTo(rowData.getField(4));
        assertThat(bigPbMessage.getFloatField6()).isEqualTo(rowData.getField(5));
        assertThat(bigPbMessage.getUint32Field7()).isEqualTo(rowData.getField(6));
        assertThat(bigPbMessage.getInt64Field8()).isEqualTo(rowData.getField(7));
        assertThat(bigPbMessage.getUint64Field9()).isEqualTo(rowData.getField(8));
        assertThat(bigPbMessage.getBytesField10().toByteArray())
                .isEqualTo(((byte[]) rowData.getField(9)));
        assertThat(bigPbMessage.getDoubleField11()).isEqualTo(rowData.getField(10));
        assertThat(bigPbMessage.getBytesField12().toByteArray())
                .isEqualTo(((byte[]) rowData.getField(11)));
        assertThat(bigPbMessage.getBoolField13()).isEqualTo(rowData.getField(12));
        assertThat(bigPbMessage.getStringField14()).isEqualTo(rowData.getField(13).toString());
        assertThat(bigPbMessage.getFloatField15()).isEqualTo(rowData.getField(14));
        assertThat(bigPbMessage.getInt32Field16()).isEqualTo(rowData.getField(15));
        assertThat(bigPbMessage.getBytesField17().toByteArray())
                .isEqualTo(((byte[]) rowData.getField(16)));
        assertThat(bigPbMessage.getBoolField18()).isEqualTo(rowData.getField(17));
        assertThat(bigPbMessage.getStringField19()).isEqualTo(rowData.getField(18).toString());
        assertThat(bigPbMessage.getFloatField20()).isEqualTo(rowData.getField(19));
        assertThat(bigPbMessage.getFixed32Field21()).isEqualTo(rowData.getField(20));
        assertThat(bigPbMessage.getFixed64Field22()).isEqualTo(rowData.getField(21));
        assertThat(bigPbMessage.getSfixed32Field23()).isEqualTo(rowData.getField(22));
        assertThat(bigPbMessage.getSfixed64Field24()).isEqualTo(rowData.getField(23));
        assertThat(bigPbMessage.getDoubleField25()).isEqualTo(rowData.getField(24));
        assertThat(bigPbMessage.getUint32Field26()).isEqualTo(rowData.getField(25));
        assertThat(bigPbMessage.getUint64Field27()).isEqualTo(rowData.getField(26));
        assertThat(bigPbMessage.getBoolField28()).isEqualTo(rowData.getField(27));
        assertThat(bigPbMessage.getField29List().get(0))
                .isEqualTo(((GenericArrayData) rowData.getField(28)).getString(0).toString());
        assertThat(bigPbMessage.getField29List().get(1))
                .isEqualTo(((GenericArrayData) rowData.getField(28)).getString(1).toString());
        assertThat(bigPbMessage.getField29List().get(2))
                .isEqualTo(((GenericArrayData) rowData.getField(28)).getString(2).toString());
        assertThat(bigPbMessage.getFloatField30()).isEqualTo(rowData.getField(29));
        assertThat(bigPbMessage.getStringField31()).isEqualTo(rowData.getField(30).toString());

        ArrayData keySet = rowData.getMap(32).keyArray();
        ArrayData valueSet = rowData.getMap(32).valueArray();
        assertThat(keySet.getString(0).toString()).isEqualTo("key2");
        assertThat(keySet.getString(1).toString()).isEqualTo("key3");
        assertThat(keySet.getString(2).toString()).isEqualTo("key1");
        assertThat(valueSet.getString(0).toString()).isEqualTo("value2");
        assertThat(valueSet.getString(1).toString()).isEqualTo("value3");
        assertThat(valueSet.getString(2).toString()).isEqualTo("value1");
    }

    @Test
    void testSplitInSerialization() throws Exception {
        RowType rowType = PbToRowTypeUtil.generateRowType(BigPbClass.BigPbMessage.getDescriptor());
        PbFormatConfig formatConfig =
                new PbFormatConfig(BigPbClass.BigPbMessage.class.getName(), false, false, "");
        PbRowDataSerializationSchema pbRowDataSerializationSchema =
                new PbRowDataSerializationSchema(rowType, formatConfig);
        pbRowDataSerializationSchema.open(null);
        // make sure code is split
        assertThat(pbRowDataSerializationSchema.isCodeSplit()).isTrue();
    }
}
