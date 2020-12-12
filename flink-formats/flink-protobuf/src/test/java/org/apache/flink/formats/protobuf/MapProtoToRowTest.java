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

import org.apache.flink.formats.protobuf.testproto.MapTest;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;

import com.google.protobuf.ByteString;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Test conversion of proto map data to flink internal data. */
public class MapProtoToRowTest {
    @Test
    public void testMessage() throws Exception {
        MapTest.InnerMessageTest innerMessageTest =
                MapTest.InnerMessageTest.newBuilder().setA(1).setB(2).build();
        MapTest mapTest =
                MapTest.newBuilder()
                        .setA(1)
                        .putMap1("a", "b")
                        .putMap1("c", "d")
                        .putMap2("f", innerMessageTest)
                        .putMap3("e", ByteString.copyFrom(new byte[] {1, 2, 3}))
                        .build();
        RowData row = ProtobufTestHelper.pbBytesToRow(MapTest.class, mapTest.toByteArray());

        MapData map1 = row.getMap(1);
        assertEquals("a", map1.keyArray().getString(0).toString());
        assertEquals("b", map1.valueArray().getString(0).toString());
        assertEquals("c", map1.keyArray().getString(1).toString());
        assertEquals("d", map1.valueArray().getString(1).toString());

        MapData map2 = row.getMap(2);
        assertEquals("f", map2.keyArray().getString(0).toString());
        RowData rowData2 = map2.valueArray().getRow(0, 2);

        assertEquals(1, rowData2.getInt(0));
        assertEquals(2L, rowData2.getLong(1));

        MapData map3 = row.getMap(3);
        assertEquals("e", map3.keyArray().getString(0).toString());
        assertArrayEquals(new byte[] {1, 2, 3}, map3.valueArray().getBinary(0));
    }
}
