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

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#JSON_QUOTE}. */
@Internal
public class JsonQuoteFunction extends BuiltInScalarFunction {

    public JsonQuoteFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.JSON_QUOTE, context);
    }

    public @Nullable Object eval(Object input) {
        if (input == null) {
            return null;
        }
        BinaryStringData bs = (BinaryStringData) input;
        String stringWithoutQuotes = quote(bs.toString());
        String outputVal = String.format("\"%s\"", stringWithoutQuotes);
        return new BinaryStringData(outputVal);
    }

    private static String quote(String input) {
        StringBuilder outputStr = new StringBuilder();

        for (int i = 0; i < input.length(); i++) {
            int codePoint = input.codePointAt(i);
            if (codePoint < 128) {
                appendASCII(outputStr, (char) codePoint);
            } else {
                outputStr.append(String.format("\\u%04x", codePoint));
            }
        }
        return outputStr.toString();
    }

    private static void appendASCII(StringBuilder outputStr, char ch) {
        switch (ch) {
            case '"':
                outputStr.append("\\\"");
                break;
            case '\\':
                outputStr.append("\\\\");
                break;
            case '/':
                outputStr.append("\\/");
                break;
            case '\b':
                outputStr.append("\\b");
                break;
            case '\f':
                outputStr.append("\\f");
                break;
            case '\n':
                outputStr.append("\\n");
                break;
            case '\r':
                outputStr.append("\\r");
                break;
            case '\t':
                outputStr.append("\\t");
                break;
            default:
                outputStr.append(ch);
        }
    }
}
