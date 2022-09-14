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

package org.apache.flink.connector.pulsar.common.schema.factories;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchemaFactory;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The schema factory for pulsar's primitive types. Currently, Pulsar supports <a
 * href="https://pulsar.apache.org/docs/en/schema-understand/#primitive-type">these primitive
 * types</a>.
 */
public class PrimitiveSchemaFactory<T> implements PulsarSchemaFactory<T> {

    private static final ImmutableSet<SchemaType> PRIMITIVE_SCHEMA_TYPES =
            ImmutableSet.<SchemaType>builder()
                    .add(SchemaType.NONE)
                    .add(SchemaType.BOOLEAN)
                    .add(SchemaType.INT8)
                    .add(SchemaType.INT16)
                    .add(SchemaType.INT32)
                    .add(SchemaType.INT64)
                    .add(SchemaType.FLOAT)
                    .add(SchemaType.DOUBLE)
                    .add(SchemaType.BYTES)
                    .add(SchemaType.STRING)
                    .add(SchemaType.TIMESTAMP)
                    .add(SchemaType.TIME)
                    .add(SchemaType.DATE)
                    .add(SchemaType.INSTANT)
                    .add(SchemaType.LOCAL_DATE)
                    .add(SchemaType.LOCAL_TIME)
                    .add(SchemaType.LOCAL_DATE_TIME)
                    .build();

    private final SchemaType type;
    private final Schema<T> schema;
    private final TypeInformation<T> typeInformation;

    public PrimitiveSchemaFactory(Schema<T> schema, TypeInformation<T> typeInformation) {
        this(schema.getSchemaInfo().getType(), schema, typeInformation);
    }

    public PrimitiveSchemaFactory(
            SchemaType type, Schema<T> schema, TypeInformation<T> typeInformation) {
        checkArgument(PRIMITIVE_SCHEMA_TYPES.contains(type));

        this.type = type;
        this.schema = schema;
        this.typeInformation = typeInformation;
    }

    @Override
    public SchemaType type() {
        return type;
    }

    @Override
    public Schema<T> createSchema(SchemaInfo info) {
        return schema;
    }

    @Override
    public TypeInformation<T> createTypeInfo(SchemaInfo info) {
        return typeInformation;
    }
}
