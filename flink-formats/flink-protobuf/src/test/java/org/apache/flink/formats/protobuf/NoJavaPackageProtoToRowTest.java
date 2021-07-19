package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.deserialize.PbRowDataDeserializationSchema;
import org.apache.flink.formats.protobuf.proto.SimpleTestNoJavaPackage;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

/** test no java_package. */
public class NoJavaPackageProtoToRowTest {
    @Test
    public void testMessage() throws Exception {
        RowType rowType =
                PbRowTypeInformationUtil.generateRowType(SimpleTestNoJavaPackage.getDescriptor());
        PbFormatConfig formatConfig =
                new PbFormatConfig(SimpleTestNoJavaPackage.class.getName(), false, false, "");
        PbRowDataDeserializationSchema deserializationSchema =
                new PbRowDataDeserializationSchema(
                        rowType, InternalTypeInfo.of(rowType), formatConfig);

        SimpleTestNoJavaPackage simple = SimpleTestNoJavaPackage.newBuilder().build();

        RowData row = deserializationSchema.deserialize(simple.toByteArray());
        row =
                ProtobufTestHelper.validateRow(
                        row,
                        PbRowTypeInformationUtil.generateRowType(
                                SimpleTestNoJavaPackage.getDescriptor()));
    }
}
