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
import org.apache.flink.table.runtime.functions.SqlJsonUtils;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#JSON_UNQUOTE}. */
@Internal
public class JsonUnquoteFunction extends BuiltInScalarFunction {

    public JsonUnquoteFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.JSON_UNQUOTE, context);
    }

    public @Nullable Object eval(Object input) {
        if (input == null) {
            return null;
        }
        BinaryStringData bs = (BinaryStringData) input;
        String inputStr = bs.toString();
        try {
            if (isValidJsonVal(inputStr)) {
                return new BinaryStringData(unescapeValidJson(inputStr));
            }
        } catch (IllegalArgumentException e) {
            // ignore exceptions on malformed input only
        }
        // return input as-is, either JSON is invalid or we encountered an exception while unquoting
        return new BinaryStringData(inputStr);
    }

    private static boolean isValidJsonVal(String jsonInString) {
        // See also BuiltInMethods.scala, IS_JSON_VALUE
        return SqlJsonUtils.isJsonValue(jsonInString);
    }

    private static String fromUnicodeLiteral(String input, int curPos) {

        StringBuilder number = new StringBuilder();
        // isValidJsonVal will already check for unicode literal validity
        for (char ch : input.substring(curPos, curPos + 4).toCharArray()) {
            number.append(Character.toLowerCase(ch));
        }
        int code = Integer.parseInt(number.toString(), 16);
        return String.valueOf((char) code);
    }

    private String unescapeStr(String inputStr) {
        StringBuilder result = new StringBuilder();
        int i = 0;
        while (i < inputStr.length()) {
            if (inputStr.charAt(i) == '\\' && i + 1 < inputStr.length()) {
                i++; // move to the next char
                char ch = inputStr.charAt(i++);

                switch (ch) {
                    case '"':
                        result.append(ch);
                        break;
                    case '\\':
                        result.append(ch);
                        break;
                    case '/':
                        result.append(ch);
                        break;
                    case 'b':
                        result.append('\b');
                        break;
                    case 'f':
                        result.append('\f');
                        break;
                    case 'n':
                        result.append('\n');
                        break;
                    case 'r':
                        result.append('\r');
                        break;
                    case 't':
                        result.append('\t');
                        break;
                    case 'u':
                        result.append(fromUnicodeLiteral(inputStr, i));
                        i = i + 4;
                        break;
                    default:
                        throw new IllegalArgumentException("Illegal escape sequence: \\" + ch);
                }
            } else {
                result.append(inputStr.charAt(i));
                i++;
            }
        }
        return result.toString();
    }

    private String unescapeValidJson(String inputStr) {
        // check for a quoted json string val and unescape
        if (inputStr.charAt(0) == '"' && inputStr.charAt(inputStr.length() - 1) == '"') {
            // remove quotes, string len is atleast 2 here
            return unescapeStr(inputStr.substring(1, inputStr.length() - 1));
        } else {
            // string representing Json - array, object or unquoted scalar val, return as-is
            return inputStr;
        }
    }
}
