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
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#JSON_UNQUOTE}. */
@Internal
public class JsonUnquoteFunction extends BuiltInScalarFunction {

    public JsonUnquoteFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.JSON_UNQUOTE, context);
    }

    private static boolean isValidJsonVal(String jsonInString) {
        // See also BuiltInMethods.scala, IS_JSON_VALUE
        return SqlJsonUtils.isJsonValue(jsonInString);
    }

    private String unquote(String inputStr) {

        StringBuilder result = new StringBuilder();
        int i = 1;
        while (i < inputStr.length() - 1) {
            if (inputStr.charAt(i) == '\\' && i + 1 < inputStr.length()) {
                i++; // move to the next char
                char charAfterBackSlash = inputStr.charAt(i++);

                switch (charAfterBackSlash) {
                    case '"':
                        result.append(charAfterBackSlash);
                        break;
                    case '\\':
                        result.append(charAfterBackSlash);
                        break;
                    case '/':
                        result.append(charAfterBackSlash);
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
                        throw new RuntimeException(
                                "Illegal escape sequence: \\" + charAfterBackSlash);
                }
            } else {
                result.append(inputStr.charAt(i));
                i++;
            }
        }
        return result.toString();
    }

    private static String fromUnicodeLiteral(String input, int curPos) {

        StringBuilder number = new StringBuilder();
        // isValidJsonVal will already check for unicode literal length >=4 and valid digit
        // composition
        for (char ch : input.substring(curPos, curPos + 4).toCharArray()) {
            number.append(Character.toLowerCase(ch));
        }
        int code = Integer.parseInt(number.toString(), 16);
        return String.valueOf((char) code);
    }

    public @Nullable Object eval(Object input) {

        try {
            if (input == null) {
                return null;
            }
            BinaryStringData bs = (BinaryStringData) input;
            String inputStr = bs.toString();
            if (isValidJsonVal(inputStr)) {
                return new BinaryStringData(unquote(inputStr));
            } else {
                // return input as is since JSON is invalid
                return new BinaryStringData(inputStr);
            }
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }
}
