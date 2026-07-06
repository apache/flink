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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#JSON_LENGTH}. */
@Internal
public class JsonLengthFunction extends BuiltInScalarFunction {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public JsonLengthFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.JSON_LENGTH, context);
    }

    public @Nullable Integer eval(@Nullable StringData jsonInput) {
        if (jsonInput == null) {
            return null;
        }
        final JsonNode jsonNode = parse(jsonInput.toString());
        if (jsonNode == null) {
            return null;
        }
        final int length = computeLength(jsonNode);
        return length == -1 ? null : length;
    }

    public @Nullable Integer eval(@Nullable StringData jsonInput, @Nullable StringData path) {
        if (jsonInput == null || path == null) {
            return null;
        }
        try {
            return SqlJsonUtils.jsonLength(jsonInput.toString(), path.toString());
        } catch (Exception e) {
            return null;
        }
    }

    private static int computeLength(JsonNode jsonNode) {
        if (jsonNode.isArray() || jsonNode.isObject()) {
            return jsonNode.size();
        } else if (jsonNode.isTextual()
                || jsonNode.isNumber()
                || jsonNode.isBoolean()
                || jsonNode.isBinary()) {
            return 1;
        }
        return -1;
    }

    private static @Nullable JsonNode parse(String jsonStr) {
        try {
            return MAPPER.readTree(jsonStr);
        } catch (Exception e) {
            return null;
        }
    }
}
