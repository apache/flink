package org.apache.flink.formats.protobuf.registry.confluent.debezium;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.protobuf.registry.confluent.ProtoToRowDataConverters;
import org.apache.flink.formats.protobuf.registry.confluent.SchemaRegistryCoder;
import org.apache.flink.formats.protobuf.registry.confluent.SchemaRegistryConfig;
import org.apache.flink.formats.protobuf.registry.confluent.utils.MutableByteArrayInputStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.common.utils.ByteUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;

import static java.lang.String.format;

public class DebeziumProtoRegistryDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    private final SchemaRegistryConfig schemaRegistryConfig;
    private final RowType rowType;
    private final TypeInformation<RowData> producedTypeInfo;
    private transient ProtoToRowDataConverters.ProtoToRowDataConverter beforeConverter;
    private transient ProtoToRowDataConverters.ProtoToRowDataConverter afterConverter;

    private transient MutableByteArrayInputStream inputStream;

    private static final String OP_READ = "r";
    /** insert operation. */
    private static final String OP_CREATE = "c";
    /** update operation. */
    private static final String OP_UPDATE = "u";
    /** delete operation. */
    private static final String OP_DELETE = "d";

    private static final String REPLICA_IDENTITY_EXCEPTION =
            "The \"before\" field of %s message is null, "
                    + "if you are using Debezium Postgres Connector, "
                    + "please check the Postgres table has been set REPLICA IDENTITY to FULL level.";
    private SchemaRegistryCoder schemaCoder;
    private transient Descriptors.Descriptor descriptor;


    public DebeziumProtoRegistryDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> producedTypeInfo,
            SchemaRegistryConfig schemaRegistryConfig) {

        this.schemaRegistryConfig = schemaRegistryConfig;
        this.rowType = rowType;
        this.producedTypeInfo = producedTypeInfo;
    }

    private void skipMessageIndexes(MutableByteArrayInputStream inputStream) throws IOException {
        final DataInputStream dataInputStream = new DataInputStream(inputStream);
        int size = ByteUtils.readVarint(dataInputStream);
        if (size == 0) {
            // optimization
            return;
        }
        for (int i = 0; i < size; i++) {
            ByteUtils.readVarint(dataInputStream);
        }
    }


    @Override
    public void open(InitializationContext context) throws Exception {
        final SchemaRegistryClient schemaRegistryClient = schemaRegistryConfig.createClient();
        this.schemaCoder =
                new SchemaRegistryCoder(schemaRegistryConfig.getSchemaId(), schemaRegistryClient);
        final ProtobufSchema schema =
                (ProtobufSchema)
                        schemaRegistryClient.getSchemaById(schemaRegistryConfig.getSchemaId());
        this.descriptor = schema.toDescriptor();

        this.beforeConverter = ProtoToRowDataConverters.createConverter(descriptor, rowType);
        this.inputStream = new MutableByteArrayInputStream();
    }


    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {

        inputStream.setBuffer(message);
        schemaCoder.readSchema(inputStream);
        // Not sure what the message indexes are, it is some Confluent Schema Registry Protobuf
        // magic. Until we figure out what that is, let's skip it
        skipMessageIndexes(inputStream);

        final DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, inputStream);

        if (message == null || message.length == 0) {
            // skip tombstone messages
            return;
        }
        RowData before= null;
        RowData after= null;
        String op = null;

        try {
            Map<Descriptors.FieldDescriptor,Object> fieldMap = dynamicMessage.getAllFields();
            //probably not write, we need to figure out exact index of proto message in debezium
            //also convert

            for(Descriptors.FieldDescriptor fd:fieldMap.keySet()){
                if(fd.getIndex()==0){
                    before = (RowData) beforeConverter.convert(fieldMap.get(fd));
                }
                if(fd.getIndex()==1){
                    after = (RowData) afterConverter.convert(fieldMap.get(fd));
                }
                if(fd.getIndex()==2){
                    op = ((DynamicMessage) fieldMap.get(fd)).toString();
                }
            }

            if (OP_CREATE.equals(op) || OP_READ.equals(op)) {
                after.setRowKind(RowKind.INSERT);
                out.collect(after);
            } else if (OP_UPDATE.equals(op)) {
                if (before == null) {
                    throw new IllegalStateException(
                            String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
                }
                before.setRowKind(RowKind.UPDATE_BEFORE);
                after.setRowKind(RowKind.UPDATE_AFTER);
                out.collect(before);
                out.collect(after);
            } else if (OP_DELETE.equals(op)) {
                if (before == null) {
                    throw new IllegalStateException(
                            String.format(REPLICA_IDENTITY_EXCEPTION, "DELETE"));
                }
                before.setRowKind(RowKind.DELETE);
                out.collect(before);
            } else {
                throw new IOException(
                        format(
                                "Unknown \"op\" value \"%s\". The Debezium Avro message is '%s'",
                                op, new String(message)));
            }
        } catch (Throwable t) {
            // a big try catch to protect the processing.
            throw new IOException("Can't deserialize Debezium Avro message.", t);
        }

    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }
}
