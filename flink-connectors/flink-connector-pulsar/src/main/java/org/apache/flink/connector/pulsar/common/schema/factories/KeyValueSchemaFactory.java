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
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchemaFactory;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchemaTypeInformation;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.Map;

import static org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils.CLASS_INFO_PLACEHOLDER;
import static org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils.decodeClassInfo;
import static org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo.decodeKeyValueEncodingType;
import static org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo.decodeKeyValueSchemaInfo;

/** The schema factory for pulsar's {@link KeyValueSchemaImpl}. */
public class KeyValueSchemaFactory<K, V> implements PulsarSchemaFactory<KeyValue<K, V>> {

    @Override
    public SchemaType type() {
        return SchemaType.KEY_VALUE;
    }

    @Override
    public Schema<KeyValue<K, V>> createSchema(SchemaInfo info) {
        KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo = decodeKeyValueSchemaInfo(info);

        Schema<K> keySchema = PulsarSchemaUtils.createSchema(kvSchemaInfo.getKey());
        Schema<V> valueSchema = PulsarSchemaUtils.createSchema(kvSchemaInfo.getValue());
        KeyValueEncodingType encodingType = decodeKeyValueEncodingType(info);

        Schema<KeyValue<K, V>> schema = KeyValueSchemaImpl.of(keySchema, valueSchema, encodingType);

        // Append extra class name into schema info properties.
        // KeyValueSchema don't have custom properties builder method, we have to use side effects.
        SchemaInfo schemaInfo = schema.getSchemaInfo();
        Map<String, String> properties = schemaInfo.getProperties();
        properties.put(CLASS_INFO_PLACEHOLDER, KeyValue.class.getName());

        return schema;
    }

    @Override
    public TypeInformation<KeyValue<K, V>> createTypeInfo(SchemaInfo info) {
        KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo = decodeKeyValueSchemaInfo(info);

        Schema<K> keySchema = PulsarSchemaUtils.createSchema(kvSchemaInfo.getKey());
        Class<K> keyClass = decodeClassInfo(keySchema.getSchemaInfo());

        Schema<V> valueSchema = PulsarSchemaUtils.createSchema(kvSchemaInfo.getValue());
        Class<V> valueClass = decodeClassInfo(valueSchema.getSchemaInfo());

        Schema<KeyValue<K, V>> schema = createSchema(info);
        PulsarSchema<KeyValue<K, V>> pulsarSchema =
                new PulsarSchema<>(schema, keyClass, valueClass);

        return new PulsarSchemaTypeInformation<>(pulsarSchema);
    }
}
