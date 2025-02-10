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

import org.apache.flink.formats.protobuf.testproto.TimestampTestOuterNomultiProto;
import org.apache.flink.formats.protobuf.testproto.VeryBigPbClass;

import org.apache.flink.formats.protobuf.util.PbToRowTypeUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;

/**
 * Test for very huge proto definition, which may trigger some special optimizations such as code
 * splitting and java constant pool size optimization.
 *
 * <p>Implementing this test as an {@code ITCase} enables larger heap size for the test execution.
 * The current unit test execution configuration would cause {@code OutOfMemoryErrors}.
 */
public class VeryBigPbProtoToRowITCase {

    @Test
    public void testSimple() throws Exception {
        VeryBigPbClass.VeryBigPbMessage veryBigPbMessage =
                VeryBigPbClass.VeryBigPbMessage.newBuilder().build();
        // test generated code can be compiled
        DataType dataType =
                ROW(
                        FIELD("nested_field1_nested_field1_nested_field1_nested_field1"
                                + "_nested_field1_nested_field1_nested_field1_nested_field1_"
                                + "nested_field1_nested_field1_nested_field1_nested_field1_"
                                + "nested_field1_nested_field1_nested_field1", INT())
                );

        RowType schema = (RowType) dataType.getLogicalType();

        String[][] projectedField =
                new String[][] {
                        new String[] {
                                "nested_field1",
                                "nested_field1",
                                "nested_field1",
                                "nested_field1",
                                "nested_field1",
                                "nested_field1",
                                "nested_field1",
                                "nested_field1",
                                "nested_field1",
                                "nested_field1",
                                "nested_field1",
                                "nested_field1",
                                "nested_field1",
                                "nested_field1",
                                "nested_field1"
                        }
        };

        RowData rowData = ProtobufTestProjectHelper.pbBytesToRowProjected(
                schema,
                veryBigPbMessage.toByteArray(),
                new PbFormatConfig(
                        VeryBigPbClass.VeryBigPbMessage.class
                                .getName(),
                        false,
                        false,
                        ""),
                projectedField
        );

        System.out.println(rowData);
    }
}
