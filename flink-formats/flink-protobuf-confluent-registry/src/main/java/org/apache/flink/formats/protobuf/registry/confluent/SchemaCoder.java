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

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.common.utils.ByteUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface SchemaCoder {
    ProtobufSchema readSchema(InputStream in) throws IOException;

    default void initialize() throws IOException {}

    ProtobufSchema writerSchema();

    void writeSchema(OutputStream out) throws IOException;

    interface Utils {
        static void writeInt(OutputStream out, int registeredId) throws IOException {
            out.write(registeredId >>> 24);
            out.write(registeredId >>> 16);
            out.write(registeredId >>> 8);
            out.write(registeredId);
        }

        static ByteBuffer writeEmptyMessageIndexes() {
            // write empty message indices for now
            ByteBuffer buffer = ByteBuffer.allocate(ByteUtils.sizeOfVarint(0));
            ByteUtils.writeVarint(0, buffer);
            return buffer;
        }
    }
}
