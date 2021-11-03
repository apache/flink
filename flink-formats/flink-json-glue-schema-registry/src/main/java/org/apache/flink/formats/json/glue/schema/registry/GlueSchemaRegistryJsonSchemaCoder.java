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
import org.apache.flink.annotation.VisibleForTesting;

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput;
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.utils.GlueSchemaRegistryUtils;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

/**
 * Schema coder that allows reading schema that is somehow embedded into serialized record. Used by
 * {@link GlueSchemaRegistryJsonSerializationSchema} and {@link
 * GlueSchemaRegistryJsonSerializationSchema}.
 */
@PublicEvolving
public class GlueSchemaRegistryJsonSchemaCoder implements Serializable {

    private final String transportName;
    private final Map<String, Object> configs;
    private final GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade;
    private final GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade;

    /**
     * Constructor accepts transport name and configuration map for AWS Glue Schema Registry.
     *
     * @param transportName topic name or stream name etc.
     * @param configs configurations for AWS Glue Schema Registry
     */
    public GlueSchemaRegistryJsonSchemaCoder(
            final String transportName, final Map<String, Object> configs) {
        this.transportName = transportName;
        this.configs = configs;
        this.glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .credentialProvider(DefaultCredentialsProvider.builder().build())
                        .glueSchemaRegistryConfiguration(
                                new GlueSchemaRegistryConfiguration(configs))
                        .build();
        this.glueSchemaRegistryDeserializationFacade =
                GlueSchemaRegistryDeserializationFacade.builder()
                        .credentialProvider(DefaultCredentialsProvider.builder().build())
                        .configs(configs)
                        .build();
    }

    @VisibleForTesting
    protected GlueSchemaRegistryJsonSchemaCoder(
            final String transportName,
            final Map<String, Object> configs,
            GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade,
            GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade) {
        this.transportName = transportName;
        this.configs = configs;
        this.glueSchemaRegistrySerializationFacade = glueSchemaRegistrySerializationFacade;
        this.glueSchemaRegistryDeserializationFacade = glueSchemaRegistryDeserializationFacade;
    }

    public Object deserialize(byte[] bytes) {
        return glueSchemaRegistryDeserializationFacade.deserialize(
                AWSDeserializerInput.builder()
                        .transportName(transportName)
                        .buffer(ByteBuffer.wrap(bytes))
                        .build());
    }

    public byte[] registerSchemaAndSerialize(Object object) {
        String schema =
                glueSchemaRegistrySerializationFacade.getSchemaDefinition(DataFormat.JSON, object);
        String schemaName = GlueSchemaRegistryUtils.getInstance().getSchemaName(configs);
        schemaName =
                schemaName != null
                        ? schemaName
                        : GlueSchemaRegistryUtils.getInstance()
                                .configureSchemaNamingStrategy(configs)
                                .getSchemaName(transportName);
        UUID schemaVersionId =
                glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(
                        AWSSerializerInput.builder()
                                .dataFormat(DataFormat.JSON.name())
                                .schemaDefinition(schema)
                                .schemaName(schemaName)
                                .transportName(transportName)
                                .build());
        return glueSchemaRegistrySerializationFacade.serialize(
                DataFormat.JSON, object, schemaVersionId);
    }
}
