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
import org.apache.flink.formats.protobuf.serialize.PbRowDataSerializationSchema;
import org.apache.flink.formats.protobuf.testproto.RecursiveMessageProto2Test;
import org.apache.flink.formats.protobuf.testproto.RecursiveMessageTest;
import org.apache.flink.formats.protobuf.testproto.RecursiveRepeatedMessageTest;
import org.apache.flink.formats.protobuf.testproto.RecursiveRequiredMessageProto2Test;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.formats.protobuf.util.PbToRowTypeUtil;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test serialization (row -&gt; proto) of recursive protobuf message types. Mirror of {@link
 * RecursiveMessageProtoToRowTest}, exercising the serialize side added alongside the deserialize
 * side in FLINK-34620.
 */
public class RecursiveMessageRowToProtoTest {

    private static final String PROTO3_CLASS =
            "org.apache.flink.formats.protobuf.testproto.RecursiveMessageTest";
    private static final String PROTO2_CLASS =
            "org.apache.flink.formats.protobuf.testproto.RecursiveMessageProto2Test";
    private static final String REPEATED_RECURSIVE_CLASS =
            "org.apache.flink.formats.protobuf.testproto.RecursiveRepeatedMessageTest";
    private static final String PROTO2_REQUIRED_CLASS =
            "org.apache.flink.formats.protobuf.testproto.RecursiveRequiredMessageProto2Test";

    /** Serializes a flink row through the full codegen pipeline. */
    private byte[] serialize(String className, RowData row) throws Exception {
        RowType rowType = PbToRowTypeUtil.generateRowType(PbFormatUtils.getDescriptor(className));
        PbFormatConfig config = new PbFormatConfig(className, false, false, "");
        PbRowDataSerializationSchema schema = new PbRowDataSerializationSchema(rowType, config);
        schema.open(null);
        return schema.serialize(row);
    }

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

    @Test
    public void testProto3RecursiveFieldSerializedFromBytes() throws Exception {
        // The recursive 'parent' field is represented as BYTES in the row, holding a serialized
        // RecursiveMessageTest (mirrors how PbCodegenBytesDeserializer emits it on read).
        RecursiveMessageTest grandparent =
                RecursiveMessageTest.newBuilder().setId(1).setName("grandparent").build();
        RecursiveMessageTest parent =
                RecursiveMessageTest.newBuilder()
                        .setId(2)
                        .setName("parent")
                        .setParent(grandparent)
                        .build();

        GenericRowData row =
                GenericRowData.of(3, StringData.fromString("child"), parent.toByteArray());

        RecursiveMessageTest result = RecursiveMessageTest.parseFrom(serialize(PROTO3_CLASS, row));

        assertEquals(3, result.getId());
        assertEquals("child", result.getName());
        assertTrue(result.hasParent());
        assertEquals(2, result.getParent().getId());
        assertEquals("parent", result.getParent().getName());
        assertTrue(result.getParent().hasParent());
        assertEquals(1, result.getParent().getParent().getId());
        assertEquals("grandparent", result.getParent().getParent().getName());
    }

    @Test
    public void testProto3NullRecursiveFieldOmitted() throws Exception {
        // The common case (e.g. signals): the recursive field is simply not populated.
        GenericRowData row = GenericRowData.of(7, StringData.fromString("leaf"), null);

        RecursiveMessageTest result = RecursiveMessageTest.parseFrom(serialize(PROTO3_CLASS, row));

        assertEquals(7, result.getId());
        assertEquals("leaf", result.getName());
        // Unset recursive field -> default instance.
        assertFalse(result.hasParent());
        assertEquals(0, result.getParent().getId());
        assertEquals("", result.getParent().getName());
    }

    @Test
    public void testProto2RecursiveFieldSerializedFromBytes() throws Exception {
        RecursiveMessageProto2Test parent =
                RecursiveMessageProto2Test.newBuilder().setId(1).setName("parent").build();

        GenericRowData row =
                GenericRowData.of(2, StringData.fromString("child"), parent.toByteArray());

        RecursiveMessageProto2Test result =
                RecursiveMessageProto2Test.parseFrom(serialize(PROTO2_CLASS, row));

        assertEquals(2, result.getId());
        assertEquals("child", result.getName());
        assertTrue(result.hasParent());
        assertEquals(1, result.getParent().getId());
        assertEquals("parent", result.getParent().getName());
    }

    @Test
    public void testProto3RepeatedRecursiveFieldsSerializedFromBytes() throws Exception {
        RowType rowType =
                PbToRowTypeUtil.generateRowType(
                        PbFormatUtils.getDescriptor(REPEATED_RECURSIVE_CLASS));
        ArrayType parentsType = (ArrayType) rowType.getTypeAt(1);
        ArrayType componentsType = (ArrayType) rowType.getTypeAt(2);
        RowType componentRowType = (RowType) componentsType.getElementType();

        assertEquals(LogicalTypeRoot.VARBINARY, parentsType.getElementType().getTypeRoot());
        assertEquals(LogicalTypeRoot.VARBINARY, componentRowType.getTypeAt(1).getTypeRoot());

        RecursiveRepeatedMessageTest parent =
                RecursiveRepeatedMessageTest.newBuilder().setId(1).build();
        RecursiveRepeatedMessageTest componentParent =
                RecursiveRepeatedMessageTest.newBuilder().setId(2).build();

        GenericArrayData parents = new GenericArrayData(new Object[] {parent.toByteArray()});
        GenericArrayData components =
                new GenericArrayData(
                        new Object[] {
                            GenericRowData.of(
                                    StringData.fromString("component-a"),
                                    componentParent.toByteArray()),
                            GenericRowData.of(StringData.fromString("component-b"), null)
                        });
        GenericRowData row = GenericRowData.of(3, parents, components);

        RecursiveRepeatedMessageTest result =
                RecursiveRepeatedMessageTest.parseFrom(serialize(REPEATED_RECURSIVE_CLASS, row));

        assertEquals(3, result.getId());
        assertEquals(1, result.getParentsCount());
        assertEquals(1, result.getParents(0).getId());
        assertEquals(2, result.getComponentsCount());
        assertEquals("component-a", result.getComponents(0).getName());
        assertTrue(result.getComponents(0).hasComponentParent());
        assertEquals(2, result.getComponents(0).getComponentParent().getId());
        assertEquals("component-b", result.getComponents(1).getName());
        assertFalse(result.getComponents(1).hasComponentParent());
    }

    // --- proto -> row -> proto round trips through the real (de)serialization schemas ---
    // Unlike the tests above (which hand-build the row), the row here comes from
    // PbRowDataDeserializationSchema, so an absent recursive field has already been converted to
    // bytes/null exactly as it is in production. This is where the absent-recursive-field bug
    // (FLINK-34620) surfaces: without the deserializer guard, an absent sub-message is materialized
    // as empty bytes and round-trips back as a present default message.

    @Test
    public void testProto3AbsentRecursiveFieldRoundTripPreservesAbsence() throws Exception {
        RecursiveMessageTest input =
                RecursiveMessageTest.newBuilder().setId(7).setName("leaf").build();

        RowData row = deserialize(PROTO3_CLASS, false, input.toByteArray());
        RecursiveMessageTest result = RecursiveMessageTest.parseFrom(serialize(PROTO3_CLASS, row));

        assertEquals(7, result.getId());
        assertEquals("leaf", result.getName());
        assertFalse(
                "Absent recursive field should remain absent after a proto -> row -> proto round trip",
                result.hasParent());
    }

    @Test
    public void testProto2AbsentRecursiveFieldRoundTripPreservesAbsence() throws Exception {
        // readDefaultValues=false: an absent proto2 message field must round-trip as absent. (With
        // readDefaultValues=true the absent field is materialized as a default instance per the
        // Flink protobuf format docs, so absence cannot be preserved in that mode.)
        RecursiveMessageProto2Test input =
                RecursiveMessageProto2Test.newBuilder().setId(7).setName("leaf").build();

        RowData row = deserialize(PROTO2_CLASS, false, input.toByteArray());
        RecursiveMessageProto2Test result =
                RecursiveMessageProto2Test.parseFrom(serialize(PROTO2_CLASS, row));

        assertEquals(7, result.getId());
        assertEquals("leaf", result.getName());
        assertFalse(
                "Absent recursive field should remain absent after a proto -> row -> proto round trip",
                result.hasParent());
    }

    @Test
    public void testProto3PresentRecursiveChainRoundTrip() throws Exception {
        // A populated recursive chain must survive proto -> row -> proto unchanged.
        RecursiveMessageTest grandparent =
                RecursiveMessageTest.newBuilder().setId(1).setName("grandparent").build();
        RecursiveMessageTest parent =
                RecursiveMessageTest.newBuilder()
                        .setId(2)
                        .setName("parent")
                        .setParent(grandparent)
                        .build();
        RecursiveMessageTest input =
                RecursiveMessageTest.newBuilder()
                        .setId(3)
                        .setName("child")
                        .setParent(parent)
                        .build();

        RowData row = deserialize(PROTO3_CLASS, false, input.toByteArray());
        RecursiveMessageTest result = RecursiveMessageTest.parseFrom(serialize(PROTO3_CLASS, row));

        assertEquals(input, result);
    }

    @Test
    public void testProto2RequiredRecursiveFieldWithReadDefaultsFailsClearly() throws Exception {
        // Unsupported combination: proto2 + recursive message + a required field in that message +
        // read-default-values=true. On read, the absent recursive field is materialized as byte[0]
        // (toByteArray() does not enforce required fields). It cannot be re-serialized, since
        // parseFrom() does enforce them. We surface a clear, actionable error rather than an opaque
        // InvalidProtocolBufferException.
        RecursiveRequiredMessageProto2Test input =
                RecursiveRequiredMessageProto2Test.newBuilder().setId(7).setName("leaf").build();

        // read-default-values=true -> the absent 'parent' becomes byte[0] in the row.
        RowData row = deserialize(PROTO2_REQUIRED_CLASS, true, input.toByteArray());

        try {
            serialize(PROTO2_REQUIRED_CLASS, row);
            fail("expected an exception for empty recursive bytes of a required message type");
        } catch (Exception e) {
            // The generated serializer runs via reflection, so our RuntimeException is wrapped
            // (InvocationTargetException, etc.). Walk the cause chain and assert our actionable
            // message appears somewhere in it.
            StringBuilder chain = new StringBuilder();
            boolean found = false;
            for (Throwable t = e; t != null; t = t.getCause()) {
                String m = t.getMessage();
                if (m != null) {
                    chain.append(m).append(" | ");
                    if (m.contains("required fields") && m.contains("read-default-values")) {
                        found = true;
                    }
                }
            }
            assertTrue(
                    "error chain should explain the required-field / read-default-values cause, got: "
                            + chain,
                    found);
        }
    }
}
