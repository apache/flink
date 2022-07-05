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
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test conversion of proto3 data to flink internal data. Default values after conversion is tested
 * especially.
 */
public class Pb3ToRowTest {
    @Test
    public void testDeserialization() throws Exception {
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

        assertEquals(1, row.getInt(0));
        assertEquals(2L, row.getLong(1));
        assertEquals("haha", row.getString(2).toString());
        assertEquals(Float.valueOf(1.1f), Float.valueOf(row.getFloat(3)));
        assertEquals(Double.valueOf(1.2), Double.valueOf(row.getDouble(4)));
        assertEquals("IMAGES", row.getString(5).toString());

        RowData rowData = row.getRow(6, 2);
        assertEquals(1, rowData.getInt(0));
        assertEquals(2L, rowData.getInt(1));

        rowData = row.getArray(7).getRow(0, 2);
        assertEquals(1, rowData.getInt(0));
        assertEquals(2L, rowData.getInt(1));

        assertEquals(100, row.getBinary(8)[0]);

        MapData map1 = row.getMap(9);
        assertEquals("a", map1.keyArray().getString(0).toString());
        assertEquals("b", map1.valueArray().getString(0).toString());
        assertEquals("c", map1.keyArray().getString(1).toString());
        assertEquals("d", map1.valueArray().getString(1).toString());

        MapData map2 = row.getMap(10);
        assertEquals("f", map2.keyArray().getString(0).toString());
        rowData = map2.valueArray().getRow(0, 2);

        assertEquals(1, rowData.getInt(0));
        assertEquals(2L, rowData.getLong(1));
    }

    @Test
    public void testReadDefaultValues() throws Exception {
        Pb3Test pb3Test = Pb3Test.newBuilder().build();
        RowData row = ProtobufTestHelper.pbBytesToRow(Pb3Test.class, pb3Test.toByteArray());

        assertFalse(row.isNullAt(0));
        assertFalse(row.isNullAt(1));
        assertFalse(row.isNullAt(2));
        assertFalse(row.isNullAt(3));
        assertFalse(row.isNullAt(4));
        assertFalse(row.isNullAt(5));
        assertFalse(row.isNullAt(6));
        assertFalse(row.isNullAt(7));
        assertFalse(row.isNullAt(8));
        assertFalse(row.isNullAt(9));
        assertFalse(row.isNullAt(10));

        assertEquals(0, row.getInt(0));
        assertEquals(0L, row.getLong(1));
        assertEquals("", row.getString(2).toString());
        assertEquals(Float.valueOf(0.0f), Float.valueOf(row.getFloat(3)));
        assertEquals(Double.valueOf(0.0d), Double.valueOf(row.getDouble(4)));
        assertEquals("UNIVERSAL", row.getString(5).toString());

        RowData rowData = row.getRow(6, 2);
        assertEquals(0, rowData.getInt(0));
        assertEquals(0L, rowData.getLong(1));

        assertEquals(0, row.getArray(7).size());

        assertEquals(0, row.getBinary(8).length);

        assertEquals(0, row.getMap(9).size());
        assertEquals(0, row.getMap(10).size());
    }
}
