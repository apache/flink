package org.apache.flink.formats.protobuf.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

import com.google.protobuf.AbstractMessage;

import java.util.ArrayList;
import java.util.List;

/** Source Function for protobuf table factory test. */
public class TestProtobufSourceFunction extends RichSourceFunction<RowData> {
    private final DeserializationSchema<RowData> deserializer;
    public static List<AbstractMessage> messages = new ArrayList<>();

    public TestProtobufSourceFunction(DeserializationSchema<RowData> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.deserializer.open(null);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        for (AbstractMessage message : messages) {
            RowData rowData = deserializer.deserialize(message.toByteArray());
            ctx.collect(rowData);
        }
    }

    @Override
    public void cancel() {}
}
