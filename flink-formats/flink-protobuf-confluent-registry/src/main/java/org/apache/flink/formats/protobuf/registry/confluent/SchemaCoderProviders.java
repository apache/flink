/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.protobuf.registry.confluent;

import org.apache.flink.util.Preconditions;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import org.apache.flink.util.WrappingRuntimeException;

import org.apache.kafka.common.utils.ByteUtils;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;
import static org.apache.flink.formats.protobuf.registry.confluent.SchemaCoder.Utils.writeEmptyMessageIndexes;
import static org.apache.flink.formats.protobuf.registry.confluent.SchemaCoder.Utils.writeInt;

public class SchemaCoderProviders {
    private static final int CONFLUENT_MAGIC_BYTE = 0;

    public static SchemaCoder get(
            int schemaId, String messageName, SchemaRegistryClient schemaRegistryClient) {
        return new PreRegisteredSchemaCoder(schemaId, messageName, schemaRegistryClient);
    }

    public static SchemaCoder get(
            String subject, ProtobufSchema rowSchema, SchemaRegistryClient schemaRegistryClient) {
        return new DefaultSchemaCoder(subject, rowSchema, schemaRegistryClient);
    }



    static class DefaultSchemaCoder implements SchemaCoder {

        private @Nullable final String subject;
        private final ProtobufSchema rowSchema;
        private final SchemaRegistryClient schemaRegistryClient;
        private static final List<Integer> DEFAULT_INDEX = Collections.singletonList(0);

        public DefaultSchemaCoder(
                @Nullable String subject,
                ProtobufSchema rowSchema,
                SchemaRegistryClient schemaRegistryClient) {
            this.subject = subject;
            this.rowSchema = Preconditions.checkNotNull(rowSchema);
            this.schemaRegistryClient = Preconditions.checkNotNull(schemaRegistryClient);

        }

        //Todo : adapted from logic
        public static MessageIndexes readMessageIndex(DataInputStream input) throws IOException {

            int size = ByteUtils.readVarint(input);
            if (size == 0) {
                return new MessageIndexes(DEFAULT_INDEX);
            } else {
                List<Integer> indexes = new ArrayList<>(size);

                for(int i = 0; i < size; ++i) {
                    indexes.add(ByteUtils.readVarint(input));
                }
                return new MessageIndexes(indexes);
            }
        }


        @Override
        public ProtobufSchema readSchema(InputStream in) throws IOException {
            DataInputStream dataInputStream = new DataInputStream(in);

            if (dataInputStream.readByte() != 0) {
                throw new IOException("Unknown data format. Magic number does not match");
            } else {
                int schemaId = dataInputStream.readInt();
                try {
                    ProtobufSchema schema = (ProtobufSchema) schemaRegistryClient.getSchemaById(schemaId);
                    MessageIndexes indexes = readMessageIndex(dataInputStream);
                    String name = schema.toMessageName(indexes);
                    schema = schema.copy(name);
                    return schema;
                } catch (RestClientException e) {
                    throw new IOException(
                            format("Could not find schema with id %s in registry", schemaId), e);
                }

            }
        }
        @Override
        public ProtobufSchema writerSchema() {
            return rowSchema;
        }


        @Override
        public void writeSchema(OutputStream out) throws IOException {
             out.write(CONFLUENT_MAGIC_BYTE);
            int schemaId = 0;
            try {
                schemaId = schemaRegistryClient.register(subject,rowSchema);
                writeInt(out, schemaId);
                final ByteBuffer buffer = writeEmptyMessageIndexes();
                out.write(buffer.array());
            } catch (RestClientException e) {
                throw new WrappingRuntimeException("Failed to serialize schema registry.", e);
            }

        }
    }

    // Todo Might need rowSchema
    static class PreRegisteredSchemaCoder implements SchemaCoder {

        private final int schemaId;
        private final @Nullable String messageName;
        private transient ProtobufSchema schema;
        private final SchemaRegistryClient schemaRegistryClient;

        public PreRegisteredSchemaCoder(
                int schemaId,
                @Nullable String messageName,
                SchemaRegistryClient schemaRegistryClient) {

            this.schemaId = schemaId;
            this.messageName = messageName;
            this.schemaRegistryClient = schemaRegistryClient;
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
        public ProtobufSchema readSchema(InputStream in) throws IOException {
            DataInputStream dataInputStream = new DataInputStream(in);

            if (dataInputStream.readByte() != 0) {
                throw new IOException("Unknown data format. Magic number does not match");
            } else {
                dataInputStream.readInt();
                skipMessageIndexes(dataInputStream);
                // return the cached schema
                return schema;
            }
        }

        @Override
        public void writeSchema(OutputStream out) throws IOException {
            // we do not check the schema, but write the id that we were initialised with
            out.write(CONFLUENT_MAGIC_BYTE);
            writeInt(out, schemaId);
            final ByteBuffer buffer = writeMessageIndexes();
            out.write(buffer.array());
        }

        private ByteBuffer writeMessageIndexes() {
            if(this.messageName!=null){
                MessageIndexes messageIndex = schema.toMessageIndexes(messageName);
                return ByteBuffer.wrap(messageIndex.toByteArray());
            }else
                return writeEmptyMessageIndexes();
        }

        @Override
        public void initialize() throws IOException {

            try {
                this.schema = (ProtobufSchema) schemaRegistryClient.getSchemaById(schemaId);
                if(this.messageName!=null){
                    //nested schema needs to be used
                    schema = schema.copy(messageName);
                }
            } catch (RestClientException e) {
                throw new IOException(
                        format("Could not find schema with id %s in registry", schemaId), e);
            }
        }

        @Override
        public ProtobufSchema writerSchema() {
            return schema;
        }
    }
}
