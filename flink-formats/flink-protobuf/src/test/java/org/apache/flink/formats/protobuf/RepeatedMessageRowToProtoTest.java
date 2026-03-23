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
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test conversion of flink internal array of row to proto data. */
class RepeatedMessageRowToProtoTest {
    @Test
    void testRepeatedMessage() throws Exception {
        RowData subRow = GenericRowData.of(1, 2L);
        RowData subRow2 = GenericRowData.of(3, 4L);
        ArrayData tmp = new GenericArrayData(new Object[] {subRow, subRow2});
        RowData row = GenericRowData.of(tmp);

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, RepeatedMessageTest.class);
        RepeatedMessageTest repeatedMessageTest = RepeatedMessageTest.parseFrom(bytes);

        assertThat(repeatedMessageTest.getDCount()).isEqualTo(2);

        assertThat(repeatedMessageTest.getD(0).getA()).isEqualTo(1);
        assertThat(repeatedMessageTest.getD(0).getB()).isEqualTo(2L);
        assertThat(repeatedMessageTest.getD(1).getA()).isEqualTo(3);
        assertThat(repeatedMessageTest.getD(1).getB()).isEqualTo(4L);
    }
}
