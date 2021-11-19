package org.apache.flink.formats.protobuf.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;

/** Sink Function for protobuf table factory test. Must run in single parallelism. */
public class TestProtobufSinkFunction extends RichSinkFunction<RowData> {
    private final SerializationSchema<RowData> serializer;
    public static List<byte[]> results = new ArrayList();

    public TestProtobufSinkFunction(SerializationSchema<RowData> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.serializer.open(null);
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        byte[] bytes = serializer.serialize(value);
        results.add(bytes);
    }
}
