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
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#JSON_UNQUOTE}. */
@Internal
public class JsonUnquoteFunction extends BuiltInScalarFunction {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public JsonUnquoteFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.JSON_UNQUOTE, context);
    }

    public @Nullable Object eval(Object input) {
        if (input == null) {
            return null;
        }
        BinaryStringData bs = (BinaryStringData) input;
        String inputStr = bs.toString();

        if (inputStr.length() > 1 && inputStr.startsWith("\"") && inputStr.endsWith("\"")) {
            inputStr = inputStr.substring(1, inputStr.length() - 1);
        }
        try {
            JsonNode jsonNode = objectMapper.readTree(inputStr);
            String res = objectMapper.writeValueAsString(jsonNode);
            return new BinaryStringData(res);
        } catch (JsonProcessingException e) {
            return input;
        }
    }
}
