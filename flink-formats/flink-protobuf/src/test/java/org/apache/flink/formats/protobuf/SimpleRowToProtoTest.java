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

import org.apache.flink.formats.protobuf.testproto.SimpleTest;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test conversion of flink internal primitive data to proto data. */
public class SimpleRowToProtoTest {
    @Test
    public void testSimple() throws Exception {
        RowData row =
                GenericRowData.of(
                        1,
                        2L,
                        false,
                        0.1f,
                        0.01,
                        StringData.fromString("hello"),
                        new byte[] {1},
                        StringData.fromString("IMAGES"),
                        1,
                        2);

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, SimpleTest.class);
        SimpleTest simpleTest = SimpleTest.parseFrom(bytes);
        assertTrue(simpleTest.hasA());
        assertEquals(1, simpleTest.getA());
        assertEquals(2L, simpleTest.getB());
        assertFalse(simpleTest.getC());
        assertEquals(Float.valueOf(0.1f), Float.valueOf(simpleTest.getD()));
        assertEquals(Double.valueOf(0.01d), Double.valueOf(simpleTest.getE()));
        assertEquals("hello", simpleTest.getF());
        assertEquals(1, simpleTest.getG().byteAt(0));
        assertEquals(SimpleTest.Corpus.IMAGES, simpleTest.getH());
        assertEquals(1, simpleTest.getFAbc7D());
    }

    @Test
    public void testNull() throws Exception {
        RowData row =
                GenericRowData.of(
                        null,
                        2L,
                        false,
                        0.1f,
                        0.01,
                        StringData.fromString("hello"),
                        null,
                        null,
                        1,
                        2);

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, SimpleTest.class);
        SimpleTest simpleTest = SimpleTest.parseFrom(bytes);
        assertFalse(simpleTest.hasA());
        assertFalse(simpleTest.hasG());
        assertFalse(simpleTest.hasH());
    }

    @Test
    public void testEnumAsInt() throws Exception {
        RowData row =
                GenericRowData.of(
                        null, null, null, null, null, null, null, 2, // CORPUS: IMAGE
                        null, null);

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, SimpleTest.class, true);
        SimpleTest simpleTest = SimpleTest.parseFrom(bytes);
        assertEquals(SimpleTest.Corpus.IMAGES, simpleTest.getH());
    }
}
