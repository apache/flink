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

import org.apache.flink.formats.protobuf.testproto.MultipleLevelMessageTest;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** Test conversion of multiple level of proto nested message data to flink internal data. */
public class MultiLevelMessageProtoToRowTest {
    @Test
    public void testMessage() throws Exception {
        MultipleLevelMessageTest.InnerMessageTest1.InnerMessageTest2 innerMessageTest2 =
                MultipleLevelMessageTest.InnerMessageTest1.InnerMessageTest2.newBuilder()
                        .setA(1)
                        .setB(2L)
                        .build();
        MultipleLevelMessageTest.InnerMessageTest1 innerMessageTest =
                MultipleLevelMessageTest.InnerMessageTest1.newBuilder()
                        .setC(false)
                        .setA(innerMessageTest2)
                        .build();
        MultipleLevelMessageTest multipleLevelMessageTest =
                MultipleLevelMessageTest.newBuilder().setD(innerMessageTest).setA(1).build();

        RowData row =
                ProtobufTestHelper.pbBytesToRow(
                        MultipleLevelMessageTest.class, multipleLevelMessageTest.toByteArray());

        assertEquals(4, row.getArity());
        RowData subRow = (RowData) row.getRow(3, 2);
        assertFalse(subRow.getBoolean(1));

        RowData subSubRow = (RowData) subRow.getRow(0, 2);
        assertEquals(1, subSubRow.getInt(0));
        assertEquals(2L, subSubRow.getLong(1));
    }
}
