package org.apache.flink.formats.protobuf.registry.confluent.debezium;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;

public class DebeziumProtoRegistrySerializationSchema implements SerializationSchema<RowData> {




    @Override
    public byte[] serialize(RowData element) {
        return new byte[0];
    }
}
