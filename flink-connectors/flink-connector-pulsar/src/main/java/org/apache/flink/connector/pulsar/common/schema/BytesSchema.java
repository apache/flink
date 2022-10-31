/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.common.schema;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static org.apache.pulsar.client.internal.PulsarClientImplementationBinding.getBytes;

/**
 * This schema is a wrapper for the original schema. It will send the schema info to Pulsar for
 * compatibility check. And didn't deserialize messages.
 */
public class BytesSchema implements Schema<byte[]>, Serializable {
    private static final long serialVersionUID = -539752264675729127L;

    private final PulsarSchema<?> schema;

    public BytesSchema(PulsarSchema<?> schema) {
        this.schema = schema;
    }

    @Override
    public void validate(byte[] message) {
        schema.getPulsarSchema().validate(message);
    }

    @Override
    public byte[] encode(byte[] message) {
        return message;
    }

    @Override
    public boolean supportSchemaVersioning() {
        return schema.getPulsarSchema().supportSchemaVersioning();
    }

    @Override
    public byte[] decode(byte[] bytes) {
        return bytes;
    }

    @Override
    public byte[] decode(byte[] bytes, byte[] schemaVersion) {
        // None of Pulsar's schema implementations have implemented this method.
        return bytes;
    }

    @Override
    public byte[] decode(ByteBuffer data, byte[] schemaVersion) {
        return getBytes(data);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schema.getSchemaInfo();
    }

    @Override
    public Schema<byte[]> clone() {
        return this;
    }
}
