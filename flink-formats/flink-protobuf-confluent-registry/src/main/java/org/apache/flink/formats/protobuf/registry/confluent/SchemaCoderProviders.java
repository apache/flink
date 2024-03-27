package org.apache.flink.formats.protobuf.registry.confluent;


import org.apache.flink.formats.protobuf.registry.confluent.utils.MutableByteArrayInputStream;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.common.utils.ByteUtils;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import static java.lang.String.format;

public class SchemaCoderProviders {
    private static final int CONFLUENT_MAGIC_BYTE = 0;


    public static SchemaCoder get(int schemaId, String messageName, SchemaRegistryClient schemaRegistryClient){
        return new SchemaRegistryCoderWithExplicitSchemaId(schemaId,messageName, schemaRegistryClient);
    }

    public static SchemaCoder get(String subject, ProtobufSchema rowSchema, SchemaRegistryClient schemaRegistryClient){
        return new DefaultSchemaRegistryCoder(subject,rowSchema, schemaRegistryClient);
    }

    static class DefaultSchemaRegistryCoder implements SchemaCoder {

        private @Nullable final String subject;
        private final ProtobufSchema rowSchema;
        private final SchemaRegistryClient schemaRegistryClient;

        public DefaultSchemaRegistryCoder(@Nullable String subject,
                                          ProtobufSchema rowSchema,
                                          SchemaRegistryClient schemaRegistryClient){
            this.subject = subject;
            this.rowSchema = rowSchema;
            this.schemaRegistryClient = Preconditions.checkNotNull(schemaRegistryClient);;
        }


        @Override
        public ParsedSchema readSchema(InputStream in) throws IOException {
            return null;
        }

        @Override
        public void writeSchema(OutputStream out) throws IOException {

        }
    }

    //Todo Might need rowSchema
    static class SchemaRegistryCoderWithExplicitSchemaId implements SchemaCoder {

        private final int schemaId;
        private final @Nullable  String messageName;
        private final SchemaRegistryClient schemaRegistryClient;
        public SchemaRegistryCoderWithExplicitSchemaId(int schemaId, @Nullable String messageName,
                                                       SchemaRegistryClient schemaRegistryClient) {

            this.schemaId = schemaId;
            this.messageName = messageName;
            this.schemaRegistryClient = schemaRegistryClient;
        }

        private static void writeInt(OutputStream out, int registeredId) throws IOException {
            out.write(registeredId >>> 24);
            out.write(registeredId >>> 16);
            out.write(registeredId >>> 8);
            out.write(registeredId);
        }

        private void skipMessageIndexes(InputStream inputStream) throws IOException {
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
        public ParsedSchema readSchema(InputStream in) throws IOException {
            DataInputStream dataInputStream = new DataInputStream(in);

            if (dataInputStream.readByte() != 0) {
                throw new IOException("Unknown data format. Magic number does not match");
            } else {
                int schemaId = dataInputStream.readInt();

                try {
                    ParsedSchema schema =  schemaRegistryClient.getSchemaById(schemaId);
                    skipMessageIndexes(in);
                    return schema;
                } catch (RestClientException e) {
                    throw new IOException(
                            format("Could not find schema with id %s in registry", schemaId), e);
                }
            }
        }

        @Override
        public void writeSchema(OutputStream out) throws IOException {
            // we do not check the schema, but write the id that we were initialised with
            out.write(CONFLUENT_MAGIC_BYTE);
            writeInt(out, schemaId);
        }

        @Override
        public void initialize(){

        }

    }

}
