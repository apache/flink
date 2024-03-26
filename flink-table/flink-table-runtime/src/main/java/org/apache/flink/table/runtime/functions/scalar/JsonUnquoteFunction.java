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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

import java.io.IOException;

/** Implementation of {@link BuiltInFunctionDefinitions#JSON_UNQUOTE}. */
@Internal
public class JsonUnquoteFunction extends BuiltInScalarFunction {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public JsonUnquoteFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.JSON_UNQUOTE, context);
    }

    public boolean isValidJSON(final String json) {
        boolean valid = false;
        try {
            final JsonParser parser = new ObjectMapper().getJsonFactory().createJsonParser(json);
            while (parser.nextToken() != null) {}
            valid = true;
        } catch (JsonParseException jpe) {
            return false;
        } catch (IOException ioe) {
            return false;
        }

        return valid;
    }

    private Tuple2<String, Boolean> unescapeUnicode(String unicode) {
        try {
            StringBuilder result = new StringBuilder();
            int i = 0;
            while (i < unicode.length()) {
                if (unicode.charAt(i) == '\\'
                        && i + 1 < unicode.length()
                        && unicode.charAt(i + 1) == 'u') {
                    String hex = unicode.substring(i + 2, i + 6);
                    char ch = (char) Integer.parseInt(hex, 16);
                    result.append(ch);
                    i += 6;
                } else {
                    result.append(unicode.charAt(i));
                    i++;
                }
            }
            return new Tuple2<>(result.toString(), true);
        } catch (NumberFormatException e) {
            return new Tuple2<>(unicode, false);
        }
    }

    private boolean isValidJson(String inputStr) {
        try {
            String jsonValStr = objectMapper.writeValueAsString(inputStr);
            if (!isValidJSON(objectMapper.writeValueAsString(inputStr))) {
                return false;
            }
            objectMapper.readTree(jsonValStr);
        } catch (JsonProcessingException e) {
            return false;
        }
        return true;
    }

    public @Nullable Object eval(Object input) {
        if (input == null) {
            return null;
        }
        BinaryStringData bs = (BinaryStringData) input;
        String inputStr = bs.toString();
        if (inputStr.length() > 1 && inputStr.startsWith("\"") && inputStr.endsWith("\"")) {
            if (!isValidJson(inputStr)) {
                return new BinaryStringData(inputStr);
            }
            Tuple2<String, Boolean> truncated =
                    unescapeUnicode(inputStr.substring(1, inputStr.length() - 1));
            return truncated.f1
                    ? new BinaryStringData(truncated.f0)
                    : new BinaryStringData(inputStr);
        } else {
            return new BinaryStringData(inputStr);
        }
    }
}
