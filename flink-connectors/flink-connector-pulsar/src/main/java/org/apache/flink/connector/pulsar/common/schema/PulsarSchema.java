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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.IOUtils;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils.createSchema;
import static org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils.decodeClassInfo;
import static org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils.encodeClassInfo;
import static org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils.haveProtobuf;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.ReflectionUtil.getTemplateType1;
import static org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo.decodeKeyValueEncodingType;
import static org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo.decodeKeyValueSchemaInfo;
import static org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo.encodeKeyValueSchemaInfo;

/**
 * A wrapper for Pulsar {@link Schema}, make it serializable and can be created from {@link
 * SchemaInfo}.
 *
 * <p>General pulsar schema info (avro, json, protobuf and keyvalue) don't contain the require class
 * info. We have to urge user to provide the related type class and encoded it into schema info.
 */
@Internal
public final class PulsarSchema<T> implements Serializable {
    private static final long serialVersionUID = -2561088131419607555L;

    private transient Schema<T> schema;
    private transient SchemaInfo schemaInfo;

    /** Create serializable pulsar schema for primitive types. */
    public PulsarSchema(Schema<T> schema) {
        SchemaInfo info = schema.getSchemaInfo();
        SchemaType type = info.getType();
        checkArgument(type != SchemaType.JSON, "Json Schema should provide the type class");
        checkArgument(type != SchemaType.AVRO, "Avro Schema should provide the type class");
        checkArgument(type != SchemaType.PROTOBUF, "Protobuf Schema should provide the type class");
        checkArgument(
                type != SchemaType.PROTOBUF_NATIVE,
                "Protobuf Native Schema should provide the type class");
        checkArgument(
                type != SchemaType.KEY_VALUE,
                "Key Value Schema should provide the type class of key and value");
        // Primitive type information could be reflected from the schema class.
        Class<?> typeClass = getTemplateType1(schema.getClass());

        this.schemaInfo = encodeClassInfo(info, typeClass);
        this.schema = createSchema(schemaInfo);
    }

    /**
     * Create serializable pulsar schema for struct type or primitive types.
     *
     * @param schema The schema instance.
     * @param typeClass The type class of this schema.
     */
    public PulsarSchema(Schema<T> schema, Class<T> typeClass) {
        SchemaInfo info = schema.getSchemaInfo();
        checkArgument(
                info.getType() != SchemaType.KEY_VALUE,
                "Key Value Schema should provide the type classes of key and value");
        validateSchemaInfo(info);

        this.schemaInfo = encodeClassInfo(info, typeClass);
        this.schema = createSchema(schemaInfo);
    }

    /** Create serializable pulsar schema for key value type. */
    public <K, V> PulsarSchema(
            Schema<KeyValue<K, V>> kvSchema, Class<K> keyClass, Class<V> valueClass) {
        SchemaInfo info = kvSchema.getSchemaInfo();
        checkArgument(
                info.getType() == SchemaType.KEY_VALUE,
                "This constructor could only be applied for KeyValueSchema");

        KeyValue<SchemaInfo, SchemaInfo> infoKeyValue = decodeKeyValueSchemaInfo(info);

        SchemaInfo infoKey = encodeClassInfo(infoKeyValue.getKey(), keyClass);
        validateSchemaInfo(infoKey);

        SchemaInfo infoValue = encodeClassInfo(infoKeyValue.getValue(), valueClass);
        validateSchemaInfo(infoValue);

        KeyValueEncodingType encodingType = decodeKeyValueEncodingType(info);
        SchemaInfo encodedInfo =
                encodeKeyValueSchemaInfo(info.getName(), infoKey, infoValue, encodingType);

        this.schemaInfo = encodeClassInfo(encodedInfo, KeyValue.class);
        this.schema = createSchema(this.schemaInfo);
    }

    public Schema<T> getPulsarSchema() {
        return schema;
    }

    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    public Class<T> getRecordClass() {
        return decodeClassInfo(schemaInfo);
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        // Name
        oos.writeUTF(schemaInfo.getName());

        // Schema
        byte[] schemaBytes = schemaInfo.getSchema();
        oos.writeInt(schemaBytes.length);
        oos.write(schemaBytes);

        // Type
        SchemaType type = schemaInfo.getType();
        oos.writeInt(type.getValue());

        // Properties
        Map<String, String> properties = schemaInfo.getProperties();
        oos.writeInt(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            oos.writeUTF(entry.getKey());
            oos.writeUTF(entry.getValue());
        }
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        // Name
        String name = ois.readUTF();

        // Schema
        int byteLen = ois.readInt();
        byte[] schemaBytes = new byte[byteLen];
        IOUtils.readFully(ois, schemaBytes, 0, byteLen);

        // Type
        int typeIdx = ois.readInt();
        SchemaType type = SchemaType.valueOf(typeIdx);

        // Properties
        int propSize = ois.readInt();
        Map<String, String> properties = new HashMap<>(propSize);
        for (int i = 0; i < propSize; i++) {
            properties.put(ois.readUTF(), ois.readUTF());
        }

        this.schemaInfo = new SchemaInfoImpl(name, schemaBytes, type, properties);
        this.schema = createSchema(schemaInfo);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaInfo that = ((PulsarSchema<?>) o).getPulsarSchema().getSchemaInfo();

        return Objects.equals(schemaInfo.getType(), that.getType())
                && Arrays.equals(schemaInfo.getSchema(), that.getSchema())
                && Objects.equals(schemaInfo.getProperties(), that.getProperties());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schemaInfo.getType(),
                Arrays.hashCode(schemaInfo.getSchema()),
                schemaInfo.getProperties());
    }

    @Override
    public String toString() {
        return schemaInfo.toString();
    }

    /**
     * We would throw exception if schema type is protobuf and user don't provide protobuf-java in
     * class path.
     */
    private void validateSchemaInfo(SchemaInfo info) {
        SchemaType type = info.getType();
        if (type == SchemaType.PROTOBUF || type == SchemaType.PROTOBUF_NATIVE) {
            checkState(
                    haveProtobuf(), "protobuf-java should be provided if you use related schema.");
        }
    }
}
