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

import org.apache.flink.annotation.VisibleForTesting;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.common.utils.ByteUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Encodes/Decodes a protobuf schema to/from an output/input stream.
 *
 * <p>The protobuf schema is encoded/decoded according to Schema Registry's wire format. All
 * components are encoded with big-endian ordering. The schema is encoded per the following wire
 * format {@code < magic-byte, schema-id, message-indexes, protobuf-payload >}
 *
 * @see <a
 *     href="https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format">
 *     Wire Format for Schema Registry</a>
 */
public abstract class SchemaCoder {
    static final int CONFLUENT_MAGIC_BYTE = 0;

    /**
     * Writes an integer value in big-endian encoding.
     *
     * @param out Outputstream to use for writing.
     * @param registeredId Integer to write.
     * @throws IOException
     */
    protected void writeInt(OutputStream out, int registeredId) throws IOException {
        out.write(registeredId >>> 24);
        out.write(registeredId >>> 16);
        out.write(registeredId >>> 8);
        out.write(registeredId);
    }

    /**
     * Writes empty message indexes to bytebuffer.
     *
     * <p>Used in cases where the schema registered in schema registry which is used for
     * serialization does not have any nested message types.
     *
     * @return ByteBuffer with empty message index.
     */
    @VisibleForTesting
    public static ByteBuffer emptyMessageIndexes() {
        ByteBuffer buffer = ByteBuffer.allocate(ByteUtils.sizeOfVarint(0));
        ByteUtils.writeVarint(0, buffer);
        return buffer;
    }

    /**
     * Infers protobuf schema from input stream.
     *
     * <p>The schema is retrieved from schemaRegistry using the schemaId which is encoded in the
     * input stream after the MAGIC_BYTE(0x0) and the Message indexes.
     *
     * @param in The Inputstream containing the encoded message and its schema.
     * @return The protobuf writer's schema used to encode the data.
     * @throws IOException in case of any malformed input/ errors in schema registry communication.
     */
    public abstract ProtobufSchema readSchema(InputStream in) throws IOException;

    /**
     * Initializes coder for use.
     *
     * <p>Schema Coder should be used only post calling initialize. Typical actions might involve
     * caching schema's in case schemaId is known upfront.
     *
     * @throws IOException
     */
    public void initialize() throws IOException {}

    /**
     * Returns the Profobuf schema used for encoding messages. Only valid for serialization
     *
     * @return Protobuf Schema used during serialization.
     */
    public abstract ProtobufSchema writerSchema();

    /**
     * Writes/Encodes the schema to outputstream.
     *
     * <p>Also responsible for writing the magic byte and the message indexes.
     *
     * @param out The Outputstream for encoding the schema.
     * @throws IOException
     */
    public abstract void writeSchema(OutputStream out) throws IOException;
}
