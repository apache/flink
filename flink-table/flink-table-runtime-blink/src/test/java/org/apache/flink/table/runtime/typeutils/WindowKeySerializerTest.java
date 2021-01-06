package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.util.WindowKey;

/** A test for the {@link WindowKeySerializer}. */
public class WindowKeySerializerTest extends SerializerTestBase<WindowKey> {

    @Override
    protected TypeSerializer<WindowKey> createSerializer() {
        return new WindowKeySerializer(2);
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<WindowKey> getTypeClass() {
        return WindowKey.class;
    }

    @Override
    protected WindowKey[] getTestData() {
        return new WindowKey[] {
            new WindowKey(1000L, createRow("11", 1)),
            new WindowKey(-2000L, createRow("12", 2)),
            new WindowKey(Long.MAX_VALUE, createRow("132", 3)),
            new WindowKey(Long.MIN_VALUE, createRow("55", 4)),
        };
    }

    private static BinaryRowData createRow(String f0, int f1) {
        BinaryRowData row = new BinaryRowData(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, StringData.fromString(f0));
        writer.writeInt(1, f1);
        writer.complete();
        return row;
    }
}
