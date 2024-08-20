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
import org.apache.flink.table.utils.ThreadLocalCache;

import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/** Implementation of {@link BuiltInFunctionDefinitions#TRANSLATE}. */
@Internal
public class TranslateFunction extends BuiltInScalarFunction {

    private static final ThreadLocalCache<Pair<String, String>, Map<Integer, String>> DICT_CACHE =
            new ThreadLocalCache<Pair<String, String>, Map<Integer, String>>() {
                @Override
                public Map<Integer, String> getNewInstance(Pair<String, String> key) {
                    return buildDict(key.getLeft(), key.getRight());
                }
            };

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

        Map<Integer, String> dict = DICT_CACHE.get(Pair.of(from, to));

        return BinaryStringData.fromString(translate(source, dict));
    }

    private String translate(String expr, Map<Integer, String> dict) {
        StringBuilder res = new StringBuilder();
        int charCount;
        for (int i = 0; i < expr.length(); i += charCount) {
            int codePoint = expr.codePointAt(i);
            charCount = Character.charCount(codePoint);
            String ch = dict.get(codePoint);
            if (ch == null) {
                res.append(expr, i, i + charCount);
            } else {
                res.append(ch);
            }
        }
        return res.toString();
    }

    private static Map<Integer, String> buildDict(String from, String to) {
        HashMap<Integer, String> hashDict = new HashMap<>();

        int i = 0;
        int j = 0;
        while (i < from.length()) {
            int toCodePoint = -1;
            int toCharCount = 1;
            if (j < to.length()) {
                toCodePoint = to.codePointAt(j);
                toCharCount = Character.charCount(toCodePoint);
                j += toCharCount;
            }

            int fromCodePoint = from.codePointAt(i);
            i += Character.charCount(fromCodePoint);

            // ignore duplicate mapping
            if (!hashDict.containsKey(fromCodePoint)) {
                hashDict.put(
                        fromCodePoint, toCodePoint == -1 ? "" : to.substring(j - toCharCount, j));
            }
        }

        return hashDict;
    }
}
