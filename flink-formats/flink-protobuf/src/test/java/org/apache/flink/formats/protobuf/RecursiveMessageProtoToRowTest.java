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

import org.apache.flink.formats.protobuf.deserialize.PbRowDataDeserializationSchema;
import org.apache.flink.formats.protobuf.testproto.RecursiveMessageProto2Test;
import org.apache.flink.formats.protobuf.testproto.RecursiveMessageTest;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.formats.protobuf.util.PbToRowTypeUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Test handling of recursive protobuf message types. */
public class RecursiveMessageProtoToRowTest {

    private static final String PROTO3_CLASS =
            "org.apache.flink.formats.protobuf.testproto.RecursiveMessageTest";
    private static final String PROTO2_CLASS =
            "org.apache.flink.formats.protobuf.testproto.RecursiveMessageProto2Test";

    /** Deserializes proto bytes through the full Flink codegen pipeline. */
    private RowData deserialize(String className, boolean readDefaultValues, byte[] data)
            throws Exception {
        RowType rowType = PbToRowTypeUtil.generateRowType(PbFormatUtils.getDescriptor(className));
        PbFormatConfig config = new PbFormatConfig(className, false, readDefaultValues, "");
        PbRowDataDeserializationSchema schema =
                new PbRowDataDeserializationSchema(rowType, InternalTypeInfo.of(rowType), config);
        schema.open(null);
        return schema.deserialize(data);
    }

    // --- schema generation ---

    @Test
    public void testCycleDetectionProducesBytes() {
        Descriptors.Descriptor descriptor = RecursiveMessageTest.getDescriptor();
        RowType rowType = PbToRowTypeUtil.generateRowType(descriptor);

        assertEquals(3, rowType.getFieldCount());
        assertEquals("id", rowType.getFieldNames().get(0));
        assertEquals("name", rowType.getFieldNames().get(1));
        assertEquals("parent", rowType.getFieldNames().get(2));
        assertEquals(
                "Recursive field should be VARBINARY",
                LogicalTypeRoot.VARBINARY,
                rowType.getTypeAt(2).getTypeRoot());
    }

    // --- proto3 deserialization ---

    @Test
    public void testProto3NestedDataPreservedAsBytes() throws Exception {
        RecursiveMessageTest grandparent =
                RecursiveMessageTest.newBuilder().setId(1).setName("grandparent").build();
        RecursiveMessageTest parent =
                RecursiveMessageTest.newBuilder()
                        .setId(2)
                        .setName("parent")
                        .setParent(grandparent)
                        .build();
        RecursiveMessageTest message =
                RecursiveMessageTest.newBuilder()
                        .setId(3)
                        .setName("child")
                        .setParent(parent)
                        .build();

        RowData row = deserialize(PROTO3_CLASS, false, message.toByteArray());
        assertNotNull(row);
        assertEquals(3, row.getInt(0));
        assertEquals("child", row.getString(1).toString());

        // Parse the bytes back - should contain full parent including grandparent
        byte[] parentBytes = row.getBinary(2);
        RecursiveMessageTest parsedParent = RecursiveMessageTest.parseFrom(parentBytes);
        assertEquals(2, parsedParent.getId());
        assertEquals("parent", parsedParent.getName());
        assertTrue(parsedParent.hasParent());
        assertEquals(1, parsedParent.getParent().getId());
        assertEquals("grandparent", parsedParent.getParent().getName());
    }

    @Test
    public void testProto3UnsetFieldReadsDefaultBytes() throws Exception {
        // In proto3, the recursive field is treated as a primitive type (VARBINARY)
        // by the codegen, so it always reads default values regardless of the
        // readDefaultValues config. Both true and false produce the same result:
        // empty bytes from .toByteArray() on the default message instance.
        RecursiveMessageTest message =
                RecursiveMessageTest.newBuilder().setId(1).setName("leaf").build();

        for (boolean readDefaults : new boolean[] {true, false}) {
            RowData row = deserialize(PROTO3_CLASS, readDefaults, message.toByteArray());
            assertNotNull(row);
            assertEquals(1, row.getInt(0));
            assertEquals("leaf", row.getString(1).toString());
            byte[] parentBytes = row.getBinary(2);
            assertNotNull("readDefaultValues=" + readDefaults, parentBytes);
            RecursiveMessageTest parsed = RecursiveMessageTest.parseFrom(parentBytes);
            assertEquals(0, parsed.getId());
            assertEquals("", parsed.getName());
        }
    }

    // --- proto2 deserialization ---

    @Test
    public void testProto2SetFieldPreservedAsBytes() throws Exception {
        RecursiveMessageProto2Test parent =
                RecursiveMessageProto2Test.newBuilder().setId(1).setName("parent").build();
        RecursiveMessageProto2Test message =
                RecursiveMessageProto2Test.newBuilder()
                        .setId(2)
                        .setName("child")
                        .setParent(parent)
                        .build();

        RowData row = deserialize(PROTO2_CLASS, false, message.toByteArray());
        assertNotNull(row);
        assertEquals(2, row.getInt(0));
        assertEquals("child", row.getString(1).toString());

        byte[] parentBytes = row.getBinary(2);
        assertNotNull(parentBytes);
        RecursiveMessageProto2Test parsed = RecursiveMessageProto2Test.parseFrom(parentBytes);
        assertEquals(1, parsed.getId());
        assertEquals("parent", parsed.getName());
    }

    @Test
    public void testProto2UnsetFieldIsNull() throws Exception {
        // Proto2 has explicit field presence. With readDefaultValues=false,
        // hasParent() returns false so the field is null.
        RecursiveMessageProto2Test message =
                RecursiveMessageProto2Test.newBuilder().setId(1).setName("leaf").build();

        RowData row = deserialize(PROTO2_CLASS, false, message.toByteArray());
        assertNotNull(row);
        assertEquals(1, row.getInt(0));
        assertEquals("leaf", row.getString(1).toString());
        assertTrue("Proto2 unset recursive field should be null", row.isNullAt(2));
    }

    @Test
    public void testProto2UnsetFieldWithReadDefaultValues() throws Exception {
        // With readDefaultValues=true, proto2 returns the default instance as bytes
        // instead of null.
        RecursiveMessageProto2Test message =
                RecursiveMessageProto2Test.newBuilder().setId(1).setName("leaf").build();

        RowData row = deserialize(PROTO2_CLASS, true, message.toByteArray());
        assertNotNull(row);
        assertEquals(1, row.getInt(0));
        assertEquals("leaf", row.getString(1).toString());
        byte[] parentBytes = row.getBinary(2);
        assertNotNull(parentBytes);
        RecursiveMessageProto2Test parsed = RecursiveMessageProto2Test.parseFrom(parentBytes);
        assertEquals(0, parsed.getId());
        assertEquals("", parsed.getName());
    }
}
