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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
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

        // pick nested field to verify the deserialization effect
        DataType dataType =
                ROW(
                        FIELD("a", INT()),
                        FIELD("d_c", BOOLEAN()),
                        FIELD("d_a_a", INT()),
                        FIELD("d_a_b", BIGINT()));
        RowType schema = (RowType) dataType.getLogicalType();
        String[][] projectedField =
                new String[][] {
                    new String[] {"a"},
                    new String[] {"d", "c"},
                    new String[] {"d", "a", "a"},
                    new String[] {"d", "a", "b"}
                };

        RowData row =
                ProtobufTestProjectHelper.pbBytesToRowProjected(
                        schema,
                        multipleLevelMessageTest.toByteArray(),
                        new PbFormatConfig(
                                MultipleLevelMessageTest.class.getName(), false, false, ""),
                        projectedField);

        assertEquals(4, row.getArity());
        assertFalse(row.getBoolean(1));
        // test the nested field A B in InnerMessageTest2
        assertEquals(1, row.getInt(2));
        assertEquals(2L, row.getLong(3));
    }
}
