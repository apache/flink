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

package org.apache.flink.formats.json.glue.schema.registry;

import org.apache.flink.annotation.PublicEvolving;

import lombok.NonNull;

import java.io.Serializable;
import java.util.Map;

/** Provider for {@link GlueSchemaRegistryJsonSchemaCoder}. */
@PublicEvolving
public class GlueSchemaRegistryJsonSchemaCoderProvider implements Serializable {

    private final String transportName;
    private final Map<String, Object> configs;

    /**
     * Constructor used by {@link GlueSchemaRegistryJsonSerializationSchema} and {@link
     * GlueSchemaRegistryJsonDeserializationSchema}.
     *
     * @param transportName topic name or stream name etc.
     * @param configs configurations for AWS Glue Schema Registry
     */
    public GlueSchemaRegistryJsonSchemaCoderProvider(
            String transportName, @NonNull Map<String, Object> configs) {
        this.transportName = transportName;
        this.configs = configs;
    }

    public GlueSchemaRegistryJsonSchemaCoder get() {
        return new GlueSchemaRegistryJsonSchemaCoder(transportName, configs);
    }
}
