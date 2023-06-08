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

package org.apache.flink.table.planner.utils;

import org.apache.flink.FlinkVersion;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/** This class contains a collection of generic utilities to deal with JSON in tests. */
public final class JsonTestUtils {

    private static final ObjectMapper OBJECT_MAPPER_INSTANCE =
            JacksonMapperFactory.createObjectMapper();

    private JsonTestUtils() {}

    public static JsonNode readFromResource(String path) throws IOException {
        return OBJECT_MAPPER_INSTANCE.readTree(JsonTestUtils.class.getResource(path));
    }

    public static JsonNode readFromString(String path) throws IOException {
        return OBJECT_MAPPER_INSTANCE.readTree(path);
    }

    public static String writeToString(JsonNode target) throws JsonProcessingException {
        return OBJECT_MAPPER_INSTANCE.writeValueAsString(target);
    }

    public static JsonNode setFlinkVersion(JsonNode target, FlinkVersion flinkVersion) {
        return ((ObjectNode) target)
                .set("flinkVersion", OBJECT_MAPPER_INSTANCE.valueToTree(flinkVersion.toString()));
    }

    public static JsonNode setExecNodeConfig(
            JsonNode target, String type, String key, String value) {
        target.get("nodes")
                .elements()
                .forEachRemaining(
                        n -> {
                            if (n.get("type").asText().equals(type)) {
                                final ObjectNode configNode =
                                        OBJECT_MAPPER_INSTANCE.createObjectNode();
                                configNode.put(key, value);
                                ((ObjectNode) n).set("configuration", configNode);
                            }
                        });
        return target;
    }

    public static JsonNode setExecNodeStateMetadata(
            JsonNode target, String type, int stateIndex, long stateTtl) {
        target.get("nodes")
                .elements()
                .forEachRemaining(
                        n -> {
                            if (n.get("type").asText().startsWith(type) && n.hasNonNull("state")) {
                                ((ObjectNode) (n.get("state").get(stateIndex)))
                                        .put("ttl", stateTtl + " ms");
                            }
                        });
        return target;
    }
}
