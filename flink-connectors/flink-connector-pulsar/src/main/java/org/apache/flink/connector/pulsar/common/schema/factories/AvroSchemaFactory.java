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

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import static org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils.decodeClassInfo;

/** The schema factory for pulsar's {@link AvroSchema}. */
public class AvroSchemaFactory<T> extends BaseStructSchemaFactory<T> {

    @Override
    public SchemaType type() {
        return SchemaType.AVRO;
    }

    @Override
    public Schema<T> createSchema(SchemaInfo info) {
        Class<T> typeClass = decodeClassInfo(info);
        SchemaDefinition<T> definition =
                SchemaDefinition.<T>builder()
                        .withPojo(typeClass)
                        .withProperties(info.getProperties())
                        .build();

        return AvroSchema.of(definition);
    }
}
