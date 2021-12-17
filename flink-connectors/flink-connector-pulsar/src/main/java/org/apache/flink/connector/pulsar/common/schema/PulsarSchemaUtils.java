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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.common.schema.factories.AvroSchemaFactory;
import org.apache.flink.connector.pulsar.common.schema.factories.JSONSchemaFactory;
import org.apache.flink.connector.pulsar.common.schema.factories.KeyValueSchemaFactory;
import org.apache.flink.connector.pulsar.common.schema.factories.PrimitiveSchemaFactory;
import org.apache.flink.connector.pulsar.common.schema.factories.ProtobufNativeSchemaFactory;
import org.apache.flink.connector.pulsar.common.schema.factories.ProtobufSchemaFactory;
import org.apache.flink.connector.pulsar.common.schema.factories.StringSchemaFactory;

import com.google.protobuf.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.BooleanSchema;
import org.apache.pulsar.client.impl.schema.ByteSchema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.DateSchema;
import org.apache.pulsar.client.impl.schema.DoubleSchema;
import org.apache.pulsar.client.impl.schema.FloatSchema;
import org.apache.pulsar.client.impl.schema.InstantSchema;
import org.apache.pulsar.client.impl.schema.IntSchema;
import org.apache.pulsar.client.impl.schema.LocalDateSchema;
import org.apache.pulsar.client.impl.schema.LocalDateTimeSchema;
import org.apache.pulsar.client.impl.schema.LocalTimeSchema;
import org.apache.pulsar.client.impl.schema.LongSchema;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.client.impl.schema.ShortSchema;
import org.apache.pulsar.client.impl.schema.TimeSchema;
import org.apache.pulsar.client.impl.schema.TimestampSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DATE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.Types.BOOLEAN;
import static org.apache.flink.api.common.typeinfo.Types.BYTE;
import static org.apache.flink.api.common.typeinfo.Types.DOUBLE;
import static org.apache.flink.api.common.typeinfo.Types.FLOAT;
import static org.apache.flink.api.common.typeinfo.Types.INSTANT;
import static org.apache.flink.api.common.typeinfo.Types.INT;
import static org.apache.flink.api.common.typeinfo.Types.LOCAL_DATE;
import static org.apache.flink.api.common.typeinfo.Types.LOCAL_DATE_TIME;
import static org.apache.flink.api.common.typeinfo.Types.LOCAL_TIME;
import static org.apache.flink.api.common.typeinfo.Types.LONG;
import static org.apache.flink.api.common.typeinfo.Types.SHORT;
import static org.apache.flink.api.common.typeinfo.Types.SQL_TIME;
import static org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Util class for pulsar schema. Register all the {@link PulsarSchemaFactory} in this class and
 * provide the {@link TypeInformation} or {@link PulsarSchema} conversion.
 */
@Internal
@SuppressWarnings("unchecked")
public final class PulsarSchemaUtils {

    private static final Class<Message> PROTOBUF_MESSAGE_CLASS;
    // Predefined pulsar schema factory.
    private static final Map<SchemaType, PulsarSchemaFactory<?>> FACTORY_REGISTER =
            new EnumMap<>(SchemaType.class);

    public static final String CLASS_INFO_PLACEHOLDER = "INTERNAL.pulsar.schema.type.class.name";

    static {
        // Initialize protobuf message class.
        Class<Message> messageClass;
        try {
            messageClass = (Class<Message>) Class.forName("com.google.protobuf.Message");
        } catch (ClassNotFoundException e) {
            messageClass = null;
        }
        PROTOBUF_MESSAGE_CLASS = messageClass;

        // Struct schemas
        registerSchemaFactory(new AvroSchemaFactory<>());
        registerSchemaFactory(new JSONSchemaFactory<>());
        registerSchemaFactory(new KeyValueSchemaFactory<>());
        if (haveProtobuf()) {
            // Protobuf type should be supported only when we have protobuf-java.
            registerSchemaFactory(new ProtobufNativeSchemaFactory<>());
            registerSchemaFactory(new ProtobufSchemaFactory<>());
        }

        // Primitive schemas
        registerSchemaFactory(new StringSchemaFactory());
        registerSchemaFactory(
                new PrimitiveSchemaFactory<>(
                        SchemaType.NONE, BytesSchema.of(), BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        registerPrimitiveFactory(BooleanSchema.of(), BOOLEAN);
        registerPrimitiveFactory(ByteSchema.of(), BYTE);
        registerPrimitiveFactory(BytesSchema.of(), BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        registerPrimitiveFactory(DateSchema.of(), DATE_TYPE_INFO);
        registerPrimitiveFactory(DoubleSchema.of(), DOUBLE);
        registerPrimitiveFactory(FloatSchema.of(), FLOAT);
        registerPrimitiveFactory(InstantSchema.of(), INSTANT);
        registerPrimitiveFactory(IntSchema.of(), INT);
        registerPrimitiveFactory(LocalDateSchema.of(), LOCAL_DATE);
        registerPrimitiveFactory(LocalDateTimeSchema.of(), LOCAL_DATE_TIME);
        registerPrimitiveFactory(LocalTimeSchema.of(), LOCAL_TIME);
        registerPrimitiveFactory(LongSchema.of(), LONG);
        registerPrimitiveFactory(ShortSchema.of(), SHORT);
        registerPrimitiveFactory(TimeSchema.of(), SQL_TIME);
        registerPrimitiveFactory(TimestampSchema.of(), SQL_TIMESTAMP);
    }

    private PulsarSchemaUtils() {
        // No need to create instance.
    }

    /** A boolean value for determine if user have protobuf-java in his class path. */
    public static boolean haveProtobuf() {
        return PROTOBUF_MESSAGE_CLASS != null;
    }

    /**
     * Check if the given class is a protobuf generated class. Since this class would throw NP
     * exception when you don't provide protobuf-java, use this method with {@link #haveProtobuf()}
     */
    public static <T> boolean isProtobufTypeClass(Class<T> clazz) {
        return checkNotNull(PROTOBUF_MESSAGE_CLASS).isAssignableFrom(clazz);
    }

    private static <T> void registerPrimitiveFactory(
            Schema<T> schema, TypeInformation<T> information) {
        registerSchemaFactory(new PrimitiveSchemaFactory<>(schema, information));
    }

    private static void registerSchemaFactory(PulsarSchemaFactory<?> factory) {
        FACTORY_REGISTER.put(factory.type(), factory);
    }

    /**
     * Pulsar has a hugh set of built-in schemas. We can create them by the given {@link
     * SchemaInfo}. This schema info is a wrapped info created by {@link PulsarSchema}.
     */
    @SuppressWarnings("unchecked")
    public static <T> Schema<T> createSchema(SchemaInfo info) {
        PulsarSchemaFactory<?> factory = FACTORY_REGISTER.get(info.getType());
        checkNotNull(factory, "This schema info %s is not supported.", info);

        return (Schema<T>) factory.createSchema(info);
    }

    /**
     * Convert the {@link SchemaInfo} into a flink manageable {@link TypeInformation}. This schema
     * info is a wrapped info created by {@link PulsarSchema}.
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeInformation<T> createTypeInformation(SchemaInfo info) {
        PulsarSchemaFactory<?> factory = FACTORY_REGISTER.get(info.getType());
        checkNotNull(factory, "This schema info %s is not supported.", info);

        return (TypeInformation<T>) factory.createTypeInfo(info);
    }

    public static SchemaInfo encodeClassInfo(SchemaInfo schemaInfo, Class<?> typeClass) {
        Map<String, String> properties = new HashMap<>(schemaInfo.getProperties());
        properties.put(CLASS_INFO_PLACEHOLDER, typeClass.getName());

        return new SchemaInfoImpl(
                schemaInfo.getName(), schemaInfo.getSchema(), schemaInfo.getType(), properties);
    }

    @SuppressWarnings("unchecked")
    public static <T> Class<T> decodeClassInfo(SchemaInfo schemaInfo) {
        Map<String, String> properties = schemaInfo.getProperties();
        String className =
                checkNotNull(
                        properties.get(CLASS_INFO_PLACEHOLDER),
                        "This schema don't contain a class name.");

        try {
            return (Class<T>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Couldn't find the schema class " + className);
        }
    }
}
