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

import org.apache.flink.formats.protobuf.testproto.RepeatedTest;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test conversion of proto list of primitive data to flink internal data. */
public class RepeatedProtoToRowTest {
    @Test
    public void testRepeated() throws Exception {
        RepeatedTest simple = RepeatedTest.newBuilder().setA(1).addB(1).addB(2).build();
        RowData row = ProtobufTestHelper.pbBytesToRow(RepeatedTest.class, simple.toByteArray());

        assertEquals(6, row.getArity());
        assertEquals(1, row.getInt(0));
        ArrayData arr = row.getArray(1);
        assertEquals(2, arr.size());
        assertEquals(1L, arr.getLong(0));
        assertEquals(2L, arr.getLong(1));
    }
}
