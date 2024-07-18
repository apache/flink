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

package org.apache.flink.protobuf.registry.confluent.dynamic;

import org.apache.flink.formats.protobuf.PbFormatConfig;

import java.io.Serializable;

/**
 * Configuration for the {@link ProtoRegistryDynamicDeserializationSchema}.
 */
public class ProtoRegistryDynamicDeserializerFormatConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String schemaRegistryUrl;
    private final boolean ignoreParseErrors;
    private final boolean readDefaultValues;

    public ProtoRegistryDynamicDeserializerFormatConfig(
            String schemaRegistryUrl,
            boolean ignoreParseErrors,
            boolean readDefaultValues) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.ignoreParseErrors = ignoreParseErrors;
        this.readDefaultValues = readDefaultValues;
    }

    public PbFormatConfig toPbFormatConfig(String messageClassName) {
        return new PbFormatConfig(messageClassName, ignoreParseErrors, readDefaultValues, null);
    }

    public boolean isIgnoreParseErrors() {
        return ignoreParseErrors;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

}
