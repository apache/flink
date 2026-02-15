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
import org.apache.flink.formats.protobuf.testproto.MapTestTruncated;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;

import com.google.protobuf.ByteString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test parse proto with unknown fields. */
public class ParseProtoWithUnknownFieldsTest {
    @Test
    public void testSimple() throws Exception {
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

        RowData row =
                ProtobufTestHelper.pbBytesToRow(MapTestTruncated.class, mapTest.toByteArray());

        // map3 is unknown in MapTestTruncated
        assertEquals(3, row.getArity());

        // inspect field a
        assertEquals(1, row.getInt(0));

        // inspect field map1
        MapData map1 = row.getMap(1);
        assertEquals("a", map1.keyArray().getString(0).toString());
        assertEquals("b", map1.valueArray().getString(0).toString());
        assertEquals("c", map1.keyArray().getString(1).toString());
        assertEquals("d", map1.valueArray().getString(1).toString());

        // inspect field map2
        MapData map2 = row.getMap(2);
        assertEquals("f", map2.keyArray().getString(0).toString());
        RowData rowData2 = map2.valueArray().getRow(0, 2);
        assertEquals(1, rowData2.getInt(0));
        assertEquals(2L, rowData2.getLong(1));
    }
}
