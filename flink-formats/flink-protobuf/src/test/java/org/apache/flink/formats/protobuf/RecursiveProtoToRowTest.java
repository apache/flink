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
import org.apache.flink.formats.protobuf.util.PbToRowTypeUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Test conversion of proto message data with recursive schema to flink internal data. */
public class RecursiveProtoToRowTest {
    @Test
    public void testRecursiveMessage() throws Exception {
        RecursiveMessageClass.RecursiveMessage message =
                RecursiveMessageClass.RecursiveMessage.newBuilder()
                        .setId(0)
                        .setMessage(
                                RecursiveMessageClass.RecursiveMessage.newBuilder()
                                        .setId(1)
                                        .setMessage(
                                                RecursiveMessageClass.RecursiveMessage.newBuilder()
                                                        .setId(2)
                                                        .setMessage(
                                                                RecursiveMessageClass
                                                                        .RecursiveMessage
                                                                        .newBuilder()
                                                                        .setId(3)
                                                                        .build())
                                                        .build())
                                        .build())
                        .build();

        int recursiveFieldMaxDepth = 2;
        RowData row =
                ProtobufTestHelper.pbBytesToRow(
                        RecursiveMessageClass.RecursiveMessage.class,
                        message.toByteArray(),
                        false,
                        recursiveFieldMaxDepth);

        RowType schema =
                PbToRowTypeUtil.generateRowType(
                        RecursiveMessageClass.RecursiveMessage.getDescriptor(), 2);
        assertEquals(0, row.getInt(0));
        assertEquals(1, row.getRow(1, 2).getInt(0));
        assertEquals(2, row.getRow(1, 2).getRow(1, 2).getInt(0));

        //       recursive field on level 2 will be removed from the schema
        assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> row.getRow(1, 2).getRow(1, 2).getRow(1, 2));
    }
}
