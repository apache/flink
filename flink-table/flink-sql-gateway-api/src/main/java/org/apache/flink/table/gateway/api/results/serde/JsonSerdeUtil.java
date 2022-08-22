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

package org.apache.flink.table.gateway.api.results.serde;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

/** A utility class that help to configure Json serialization and deserialization. */
@Internal
public class JsonSerdeUtil {

    private JsonSerdeUtil() {}

    /**
     * Object mapper shared instance to serialize and deserialize LogicalType and DataType. Note
     * that creating and copying of object mappers is expensive and should be avoided.
     */
    private static final ObjectMapper OBJECT_MAPPER_INSTANCE =
            JacksonMapperFactory.createObjectMapper();

    static {
        SimpleModule module = new SimpleModule();
        // serializer
        module.addSerializer(new JsonResultSetSerializer());
        module.addSerializer(new LogicalTypeJsonSerializer());
        // deserializer
        module.addDeserializer(ResultSet.class, new JsonResultSetDeserializer());
        module.addDeserializer(LogicalType.class, new LogicalTypeJsonDeserializer());

        OBJECT_MAPPER_INSTANCE.registerModule(module);
    }

    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER_INSTANCE;
    }
}
