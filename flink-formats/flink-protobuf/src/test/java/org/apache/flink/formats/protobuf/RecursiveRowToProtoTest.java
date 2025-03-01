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

import org.apache.flink.formats.protobuf.testproto.RecursiveMessageClass;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test conversion of folded flink internal rows to proto data. */
public class RecursiveRowToProtoTest {
    @Test
    public void testRecursiveMessage() throws Exception {

        RowData subRowLevel4 = GenericRowData.of(4);
        RowData subRowLevel3 = GenericRowData.of(3, subRowLevel4);
        RowData subRowLevel2 = GenericRowData.of(2, subRowLevel3);
        RowData subRowLevel1 = GenericRowData.of(1, subRowLevel2);
        RowData row = GenericRowData.of(99, subRowLevel1);
        int recursiveFieldMaxDepth = 2;

        byte[] bytes =
                ProtobufTestHelper.rowToPbBytes(
                        row,
                        RecursiveMessageClass.RecursiveMessage.class,
                        false,
                        recursiveFieldMaxDepth);
        RecursiveMessageClass.RecursiveMessage recursiveMessageTest =
                RecursiveMessageClass.RecursiveMessage.parseFrom(bytes);

        assertEquals(99, recursiveMessageTest.getId());
        assertEquals(1, recursiveMessageTest.getMessage().getId());
        assertEquals(2, recursiveMessageTest.getMessage().getMessage().getId());

        // recursive schema is trimmed on the level=`recursiveFieldMaxDepth+1`.
        // so on the next this will return a default value - 0
        assertEquals(0, recursiveMessageTest.getMessage().getMessage().getMessage().getId());
    }
}
