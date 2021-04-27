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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** Test conversion of flink internal nested row data to proto data. */
public class MultiLevelMessageRowToProtoTest {
    @Test
    public void testMultipleLevelMessage() throws Exception {
        RowData subSubRow = GenericRowData.of(1, 2L);
        RowData subRow = GenericRowData.of(subSubRow, false);
        RowData row = GenericRowData.of(1, 2L, false, subRow);

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, MultipleLevelMessageTest.class);
        MultipleLevelMessageTest test = MultipleLevelMessageTest.parseFrom(bytes);

        assertFalse(test.getD().getC());
        assertEquals(1, test.getD().getA().getA());
        assertEquals(2L, test.getD().getA().getB());
        assertEquals(1, test.getA());
    }

    @Test
    public void testNull() throws Exception {
        RowData row = GenericRowData.of(1, 2L, false, null);
        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, MultipleLevelMessageTest.class);

        MultipleLevelMessageTest test = MultipleLevelMessageTest.parseFrom(bytes);

        MultipleLevelMessageTest.InnerMessageTest1 empty =
                MultipleLevelMessageTest.InnerMessageTest1.newBuilder().build();
        assertEquals(empty, test.getD());
    }
}
