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
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.BinaryStringDataUtil;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/** Implementation of {@link BuiltInFunctionDefinitions#TRANSLATE}. */
@Internal
public class TranslateFunction extends BuiltInScalarFunction {

    private transient String lastFrom = "";
    private transient String lastTo = "";
    private transient Map<Integer, String> dict;

    public TranslateFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.TRANSLATE, context);
    }

    public @Nullable StringData eval(
            @Nullable StringData expr, @Nullable StringData fromStr, @Nullable StringData toStr) {
        if (expr == null
                || fromStr == null
                || BinaryStringDataUtil.isEmpty((BinaryStringData) expr)
                || BinaryStringDataUtil.isEmpty((BinaryStringData) fromStr)) {
            return expr;
        }

        final String source = expr.toString();
        final String from = fromStr.toString();
        final String to = toStr == null ? "" : toStr.toString();

        if (!from.equals(lastFrom) || !to.equals(lastTo)) {
            lastFrom = from;
            lastTo = to;
            dict = buildDict(from, to);
        }

        return BinaryStringData.fromString(translate(source));
    }

    private String translate(String expr) {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < expr.length(); ) {
            int codePoint = expr.codePointAt(i);
            i += Character.charCount(codePoint);
            String ch = dict.get(codePoint);
            if (ch == null) {
                res.append(Character.toChars(codePoint));
            } else {
                res.append(ch);
            }
        }
        return res.toString();
    }

    private Map<Integer, String> buildDict(String from, String to) {
        HashMap<Integer, String> hashDict = new HashMap<>();

        int i = 0;
        int j = 0;
        while (i < from.length()) {
            int toCodePoint = -1;
            if (j < to.length()) {
                toCodePoint = to.codePointAt(j);
                j += Character.charCount(toCodePoint);
            }

            int fromCodePoint = from.codePointAt(i);
            i += Character.charCount(fromCodePoint);

            // ignore duplicate mapping
            if (!hashDict.containsKey(fromCodePoint)) {
                hashDict.put(
                        fromCodePoint,
                        toCodePoint == -1 ? "" : String.valueOf(Character.toChars(toCodePoint)));
            }
        }

        return hashDict;
    }
}
