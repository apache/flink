package org.apache.flink.formats.protobuf.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/** */
public class TestProtobufTableSink implements DynamicTableSink {
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private final DataType dataType;

    public TestProtobufTableSink(
            EncodingFormat<SerializationSchema<RowData>> encodingFormat, DataType dataType) {
        this.encodingFormat = encodingFormat;
        this.dataType = dataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> serializer =
                encodingFormat.createRuntimeEncoder(context, dataType);
        return SinkFunctionProvider.of(new TestProtobufSinkFunction(serializer));
    }

    @Override
    public DynamicTableSink copy() {
        return new TestProtobufTableSink(encodingFormat, dataType);
    }

    @Override
    public String asSummaryString() {
        return TestProtobufTableSink.class.getName();
    }
}
