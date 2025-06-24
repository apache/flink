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

import org.apache.flink.formats.protobuf.testproto.RepeatedMessageTest;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test conversion of proto repeated message data to flink internal data. */
public class RepeatedMessageProtoToRowTest {
    @Test
    public void testRepeatedMessage() throws Exception {
        RepeatedMessageTest.InnerMessageTest innerMessageTest =
                RepeatedMessageTest.InnerMessageTest.newBuilder().setA(1).setB(2L).build();

        RepeatedMessageTest.InnerMessageTest innerMessageTest1 =
                RepeatedMessageTest.InnerMessageTest.newBuilder().setA(3).setB(4L).build();

        RepeatedMessageTest repeatedMessageTest =
                RepeatedMessageTest.newBuilder()
                        .addD(innerMessageTest)
                        .addD(innerMessageTest1)
                        .build();

        RowData row =
                ProtobufTestHelper.pbBytesToRow(
                        RepeatedMessageTest.class, repeatedMessageTest.toByteArray());

        ArrayData objs = row.getArray(0);
        RowData subRow = objs.getRow(0, 2);
        assertEquals(1, subRow.getInt(0));
        assertEquals(2L, subRow.getLong(1));
        subRow = objs.getRow(1, 2);
        assertEquals(3, subRow.getInt(0));
        assertEquals(4L, subRow.getLong(1));
    }
}
