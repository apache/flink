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

import org.apache.flink.formats.protobuf.registry.confluent.utils.FlinkToProtoSchemaConverter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingRuntimeException;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
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
import java.util.Objects;

import static java.lang.String.format;

/** Factory for {@link org.apache.flink.formats.protobuf.registry.confluent.SchemaCoder}. */
public class SchemaCoderProviders {

    /**
     * Creates a {@link org.apache.flink.formats.protobuf.registry.confluent.SchemaCoder} in cases
     * where the schema has already been setup before-hand/exists in Confluent Schema Registry.
     *
     * <p>Useful in scenarios where users want to be more explicit with schemas used. In these cases
     * the external schema specified through schemaId will take precedence for encoding/decoding
     * data. Also, the step of registering with schemaRegistry during serialization, will be
     * skipped.
     *
     * <p>A single Schema Registry Protobuf entry may contain multiple Protobuf messages, some of
     * which may have nested messages. The messageName identifies the exact message/schema to use
     * for serialization/deserialization. Consider the following protobuf message
     *
     * <pre>
     * package test.package;
     * message MessageA {
     *     message MessageB {
     *         message MessageC {
     *         ...
     *         }
     *     }
     *     message MessageD {
     *     ...
     *     }
     *     message MessageE {
     *         message MessageF {
     *         ...
     *         }
     *         message MessageG {
     *         ...
     *         }
     *     ...
     *     }
     * ...
     * }
     * </pre>
     *
     * <p>In order to use messageD the messageName should contain the value of
     * test.package.messageD. Similarly, for messageF to be used messageName should contain
     * test.package.MessageE.MessageF.
     *
     * @param schemaId SchemaId for external schema referenced for encoding/decoding of payload.
     * @param messageName Optional message name to be used to select the right {@link
     *     com.google.protobuf.Message} for Serialialization/Deserialization. In absence of
     *     messageName the outermost message will be used.
     * @param schemaRegistryClient client handle to Schema Registry {@link
     *     io.confluent.kafka.schemaregistry.client.SchemaRegistryClient}
     * @return
     */
    public static SchemaCoder createForPreRegisteredSchema(
            int schemaId, @Nullable String messageName, SchemaRegistryClient schemaRegistryClient) {
        return new PreRegisteredSchemaCoder(schemaId, messageName, schemaRegistryClient);
    }

    /**
     * Creates a default schema coder.
     *
     * <p>For serialization schema coder will infer the schema from Flink {@link
     * org.apache.flink.table.types.logical.RowType}. Schema obtained from rowType will also be
     * registered to Schema Registry using the subject passed in by invoking {@link
     * io.confluent.kafka.schemaregistry.client.SchemaRegistryClient#register(String,
     * io.confluent.kafka.schemaregistry.ParsedSchema)}.
     *
     * <p>For deserialization schema coder will infer schema from InputStream. In cases where
     * messageIndexes indicate using a nested schema, the appropriate nested schema will be used.
     *
     * @param subject Subject to use for registering schema (only required for serialization).
     * @param rowType Flink Row type.
     * @param schemaRegistryClient Client for SchemaRegistry
     * @return SchemaCoder to use.
     */
    public static SchemaCoder createDefault(
            String subject, RowType rowType, SchemaRegistryClient schemaRegistryClient) {
        return new DefaultSchemaCoder(subject, rowType, schemaRegistryClient);
    }

    /**
     * Default implementation of SchemaCoder.
     *
     * <p>Parses schema information from inputStream for de-serialization. For Serialization, uses
     * Flink Row Type to infer schema and registers this schema with Schema Registry.
     */
    static class DefaultSchemaCoder extends SchemaCoder {
        private static final String ROW = "row";
        private static final String PACKAGE = "io.confluent.generated";
        private static final List<Integer> DEFAULT_INDEX = Collections.singletonList(0);
        /** Subject can be nullable in case coder is only used for deserialization. */
        private @Nullable final String subject;

        private final ProtobufSchema rowSchema;
        private final SchemaRegistryClient schemaRegistryClient;

        public DefaultSchemaCoder(
                @Nullable String subject,
                RowType rowType,
                SchemaRegistryClient schemaRegistryClient) {
            this.subject = subject;

            // In case the rowType changes (due to addition or deletion of columns), the
            // schema generated might not be compatible with schema's generated earlier.
            rowSchema =
                    FlinkToProtoSchemaConverter.fromFlinkRowType(
                            Preconditions.checkNotNull(rowType), ROW, PACKAGE);
            this.schemaRegistryClient = Preconditions.checkNotNull(schemaRegistryClient);
        }

        public static MessageIndexes readMessageIndex(DataInputStream input) throws IOException {

            int size = ByteUtils.readVarint(input);
            if (size == 0) {
                return new MessageIndexes(DEFAULT_INDEX);
            } else {
                List<Integer> indexes = new ArrayList<>(size);

                for (int i = 0; i < size; ++i) {
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
                    ProtobufSchema schema =
                            (ProtobufSchema) schemaRegistryClient.getSchemaById(schemaId);
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
                schemaId = schemaRegistryClient.register(subject, rowSchema);
                writeInt(out, schemaId);
                /* We write empty indexes as schema derived from rowType will have root element only */
                final ByteBuffer buffer = emptyMessageIndexes();
                out.write(buffer.array());
            } catch (RestClientException e) {
                throw new WrappingRuntimeException("Failed to serialize schema registry.", e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DefaultSchemaCoder)) {
                return false;
            }
            DefaultSchemaCoder that = (DefaultSchemaCoder) o;
            return Objects.equals(subject, that.subject)
                    && Objects.equals(rowSchema, that.rowSchema);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subject, rowSchema);
        }
    }

    /**
     * Schema coder instance which uses externally registered schema for serialization,
     * deserialization
     *
     * <p>Useful for scenarios where schema is setup offline and not inferred from flink rowType.
     *
     * <p>Explicitly uses schema registered corresponding to schemaId passed in during
     * initialization. For deserialization, in case of nested/multiple schemas optionally takes in
     * messageName to indicate schema to use. For serialization uses pre-registered schema for
     * writing payload.
     */
    static class PreRegisteredSchemaCoder extends SchemaCoder {

        private final int schemaId;
        private final @Nullable String messageName;
        private final SchemaRegistryClient schemaRegistryClient;
        private transient ProtobufSchema registeredSchema;

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
                return registeredSchema;
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
            if (this.messageName != null) {
                MessageIndexes messageIndex = registeredSchema.toMessageIndexes(messageName);
                return ByteBuffer.wrap(messageIndex.toByteArray());
            } else {
                return emptyMessageIndexes();
            }
        }

        @Override
        public void initialize() throws IOException {

            try {
                this.registeredSchema =
                        (ProtobufSchema) schemaRegistryClient.getSchemaById(schemaId);
                if (this.messageName != null) {
                    // nested schema needs to be used
                    registeredSchema = registeredSchema.copy(messageName);
                }
            } catch (RestClientException e) {
                throw new IOException(
                        format("Could not find schema with id %s in registry", schemaId), e);
            }
        }

        @Override
        public ProtobufSchema writerSchema() {
            return registeredSchema;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PreRegisteredSchemaCoder)) {
                return false;
            }
            PreRegisteredSchemaCoder that = (PreRegisteredSchemaCoder) o;
            return schemaId == that.schemaId && Objects.equals(messageName, that.messageName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaId, messageName);
        }
    }
}
