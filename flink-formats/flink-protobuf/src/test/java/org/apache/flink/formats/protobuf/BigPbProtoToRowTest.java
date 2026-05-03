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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for huge proto definition, which may trigger some special optimizations such as code
 * splitting.
 */
class BigPbProtoToRowTest {

    @Test
    void testSimple() throws Exception {
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

        assertThat(row.getInt(0)).isEqualTo(5);
        assertThat(row.getBoolean(1)).isFalse();
        assertThat(row.getString(2).toString()).isEqualTo("test1");
        assertThat(row.getBinary(3)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(row.getDouble(4)).isEqualTo(2.5);
        assertThat(row.getFloat(5)).isEqualTo(1.5F);
        assertThat(row.getInt(6)).isEqualTo(3);
        assertThat(row.getLong(7)).isEqualTo(7L);
        assertThat(row.getLong(8)).isEqualTo(9L);
        assertThat(row.getBinary(9)).isEqualTo(new byte[] {4, 5, 6});
        assertThat(row.getDouble(10)).isEqualTo(6.5);
        assertThat(row.getBinary(11)).isEqualTo(new byte[] {7, 8, 9});
        assertThat(row.getBoolean(12)).isTrue();
        assertThat(row.getString(13).toString()).isEqualTo("test2");
        assertThat(row.getFloat(14)).isEqualTo(3.5F);
        assertThat(row.getInt(15)).isEqualTo(8);
        assertThat(row.getBinary(16)).isEqualTo(new byte[] {10, 11, 12});
        assertThat(row.getBoolean(17)).isTrue();
        assertThat(row.getString(18).toString()).isEqualTo("test3");
        assertThat(row.getFloat(19)).isEqualTo(4.5F);
        assertThat(row.getInt(20)).isEqualTo(1);
        assertThat(row.getLong(21)).isEqualTo(2L);
        assertThat(row.getInt(22)).isEqualTo(3);
        assertThat(row.getLong(23)).isEqualTo(4L);
        assertThat(row.getDouble(24)).isEqualTo(5.5);
        assertThat(row.getInt(25)).isEqualTo(6);
        assertThat(row.getLong(26)).isEqualTo(7L);
        assertThat(row.getBoolean(27)).isTrue();
        assertThat(row.getArray(28).getString(0).toString()).isEqualTo("value1");
        assertThat(row.getArray(28).getString(1).toString()).isEqualTo("value2");
        assertThat(row.getArray(28).getString(2).toString()).isEqualTo("value3");
        assertThat(row.getFloat(29)).isEqualTo(8.5F);
        assertThat(row.getString(30).toString()).isEqualTo("test4");
        assertThat(row.getMap(31).valueArray().getBinary(0)).isEqualTo(new byte[] {13, 14, 15});
        assertThat(row.getMap(31).valueArray().getBinary(1)).isEqualTo(new byte[] {16, 17, 18});
        assertThat(row.getMap(32).valueArray().getString(0).toString()).isEqualTo("value1");
        assertThat(row.getMap(32).valueArray().getString(1).toString()).isEqualTo("value2");
    }

    @Test
    void testSplitInDeserialization() throws Exception {
        RowType rowType = PbToRowTypeUtil.generateRowType(BigPbClass.BigPbMessage.getDescriptor());
        PbFormatConfig formatConfig =
                new PbFormatConfig(BigPbClass.BigPbMessage.class.getName(), false, false, "");
        PbRowDataDeserializationSchema pbRowDataDeserializationSchema =
                new PbRowDataDeserializationSchema(
                        rowType, InternalTypeInfo.of(rowType), formatConfig);
        pbRowDataDeserializationSchema.open(null);
        // make sure code is split
        assertThat(pbRowDataDeserializationSchema.isCodeSplit()).isTrue();
    }
}
