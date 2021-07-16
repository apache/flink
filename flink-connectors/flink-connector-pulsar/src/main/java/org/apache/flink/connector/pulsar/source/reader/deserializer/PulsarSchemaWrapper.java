/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.source.reader.deserializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.ReflectionUtil;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

/**
 * The deserialization schema wrapper for pulsar original {@link Schema}. Pulsar would deserialize
 * the message and pass it to flink with a auto generate or given {@link TypeInformation}.
 *
 * @param <T> The output type of the message.
 */
@Internal
class PulsarSchemaWrapper<T> extends PulsarDeserializationSchemaBase<T, T> {
    private static final long serialVersionUID = -4864701207257059158L;

    public PulsarSchemaWrapper(PulsarSchemaFactory<T> schemaFactory) {
        this(schemaFactory, createTypeInformation(schemaFactory));
    }

    public PulsarSchemaWrapper(Class<? extends PulsarSchemaFactory<T>> factoryClass) {
        this(
                ReflectionUtil.newInstance(factoryClass),
                createTypeInformation(ReflectionUtil.newInstance(factoryClass)));
    }

    public PulsarSchemaWrapper(
            PulsarSchemaFactory<T> schemaFactory, TypeInformation<T> typeInformation) {
        super(
                schemaFactory,
                message -> Collections.singletonList(message.getValue()),
                typeInformation);
    }

    /**
     * Convert the {@link Schema} into a flink readable {@link TypeInformation}. We only support all
     * the primitive types in pulsar built-in schema.
     *
     * <p>Please visit <a
     * href="ttp://pulsar.apache.org/docs/en/schema-understand/#schema-type">pulsar
     * documentation</a> for detailed schema type clarify.
     */
    @SuppressWarnings("unchecked")
    private static <T> TypeInformation<T> createTypeInformation(
            PulsarSchemaFactory<T> schemaFactory) {
        Schema<T> schema = schemaFactory.create();
        // SchemaInfo contains all the required information for deserializing.
        SchemaInfo schemaInfo = schema.getSchemaInfo();
        SchemaType schemaType = schemaInfo.getType();

        TypeInformation<?> information = null;
        switch (schemaType) {
            case STRING:
                information = Types.STRING;
                break;
            case BOOLEAN:
                information = Types.BOOLEAN;
                break;
            case INT8:
                information = Types.BYTE;
                break;
            case INT16:
                information = Types.SHORT;
                break;
            case INT32:
                information = Types.INT;
                break;
            case INT64:
                information = Types.LONG;
                break;
            case FLOAT:
                information = Types.FLOAT;
                break;
            case DOUBLE:
                information = Types.DOUBLE;
                break;
            case DATE:
                // Since pulsar use this type for both util.Date and sql.Date,
                // we just choose util.Date here.
                information = BasicTypeInfo.DATE_TYPE_INFO;
                break;
            case TIME:
                information = Types.SQL_TIME;
                break;
            case TIMESTAMP:
                information = Types.SQL_TIMESTAMP;
                break;
            case INSTANT:
                information = Types.INSTANT;
                break;
            case LOCAL_DATE:
                information = Types.LOCAL_DATE;
                break;
            case LOCAL_TIME:
                information = Types.LOCAL_TIME;
                break;
            case LOCAL_DATE_TIME:
                information = Types.LOCAL_DATE_TIME;
                break;
            case BYTES:
                information = Types.PRIMITIVE_ARRAY(Types.BYTE);
                break;
        }

        if (information == null) {
            // Try to extract the type info by using flink provided utils.
            try {
                // TODO Support protobuf class after flink support it natively.
                information =
                        TypeExtractor.createTypeInfo(
                                PulsarSchemaFactory.class, schemaFactory.getClass(), 0, null, null);
            } catch (Exception e) {
                // Nothing to do here, this exception is accepted and don't need.
            }
        }

        if (information != null) {
            return (TypeInformation<T>) information;
        } else {
            String schemaInfoStr = new String(schemaInfo.getSchema(), StandardCharsets.UTF_8);
            throw new FlinkRuntimeException(
                    "Unsupported pulsar schema, please provide the related TypeInformation. The schema info was: \n"
                            + schemaInfoStr);
        }
    }
}
