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

import org.apache.flink.formats.protobuf.testproto.Pb3Test;
import org.apache.flink.formats.protobuf.testproto.Pb3Test.Corpus;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test conversion of proto3 data to flink internal data. Default values after conversion is tested
 * especially.
 */
class Pb3ToRowTest {
    @Test
    void testDeserialization() throws Exception {
        Pb3Test.InnerMessageTest innerMessageTest =
                Pb3Test.InnerMessageTest.newBuilder().setA(1).setB(2).build();
        Pb3Test mapTest =
                Pb3Test.newBuilder()
                        .setA(1)
                        .setB(2L)
                        .setC("haha")
                        .setD(1.1f)
                        .setE(1.2)
                        .setF(Corpus.IMAGES)
                        .setG(innerMessageTest)
                        .addH(innerMessageTest)
                        .setI(ByteString.copyFrom(new byte[] {100}))
                        .putMap1("a", "b")
                        .putMap1("c", "d")
                        .putMap2("f", innerMessageTest)
                        .build();

        RowData row = ProtobufTestHelper.pbBytesToRow(Pb3Test.class, mapTest.toByteArray());

        assertThat(row.getInt(0)).isEqualTo(1);
        assertThat(row.getLong(1)).isEqualTo(2L);
        assertThat(row.getString(2).toString()).isEqualTo("haha");
        assertThat(row.getFloat(3)).isEqualTo(1.1f);
        assertThat(row.getDouble(4)).isEqualTo(1.2);
        assertThat(row.getString(5).toString()).isEqualTo("IMAGES");

        RowData rowData = row.getRow(6, 2);
        assertThat(rowData.getInt(0)).isEqualTo(1);
        assertThat(rowData.getInt(1)).isEqualTo(2L);

        rowData = row.getArray(7).getRow(0, 2);
        assertThat(rowData.getInt(0)).isEqualTo(1);
        assertThat(rowData.getInt(1)).isEqualTo(2L);

        assertThat(row.getBinary(8)[0]).isEqualTo((byte) 100);

        MapData map1 = row.getMap(9);
        assertThat(map1.keyArray().getString(0).toString()).isEqualTo("a");
        assertThat(map1.valueArray().getString(0).toString()).isEqualTo("b");
        assertThat(map1.keyArray().getString(1).toString()).isEqualTo("c");
        assertThat(map1.valueArray().getString(1).toString()).isEqualTo("d");

        MapData map2 = row.getMap(10);
        assertThat(map2.keyArray().getString(0).toString()).isEqualTo("f");
        rowData = map2.valueArray().getRow(0, 2);

        assertThat(rowData.getInt(0)).isEqualTo(1);
        assertThat(rowData.getLong(1)).isEqualTo(2L);
    }

    @Test
    void testReadDefaultValues() throws Exception {
        Pb3Test pb3Test = Pb3Test.newBuilder().build();
        RowData row = ProtobufTestHelper.pbBytesToRow(Pb3Test.class, pb3Test.toByteArray());

        // primitive types should have default values
        assertThat(row.isNullAt(0)).isFalse();
        assertThat(row.isNullAt(1)).isFalse();
        assertThat(row.isNullAt(2)).isFalse();
        assertThat(row.isNullAt(3)).isFalse();
        assertThat(row.isNullAt(4)).isFalse();
        assertThat(row.isNullAt(5)).isFalse();
        assertThat(row.isNullAt(8)).isFalse();

        assertThat(row.getInt(0)).isEqualTo(0);
        assertThat(row.getLong(1)).isEqualTo(0L);
        assertThat(row.getString(2).toString()).isEqualTo("");
        assertThat(row.getFloat(3)).isEqualTo(0.0f);
        assertThat(row.getDouble(4)).isEqualTo(0.0d);
        assertThat(row.getString(5).toString()).isEqualTo("UNIVERSAL");
        assertThat(row.getBinary(8).length).isEqualTo(0);
        // non-primitive types should be null
        assertThat(row.isNullAt(6)).isTrue();
        assertThat(row.isNullAt(7)).isTrue();
        assertThat(row.isNullAt(9)).isTrue();
        assertThat(row.isNullAt(10)).isTrue();
    }
}
