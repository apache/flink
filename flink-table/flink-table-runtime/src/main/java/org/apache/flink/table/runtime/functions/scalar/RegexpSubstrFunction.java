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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.utils.ThreadLocalCache;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Implementation of {@link BuiltInFunctionDefinitions#REGEXP_SUBSTR}. */
@Internal
public class RegexpSubstrFunction extends BuiltInScalarFunction {

    private static final ThreadLocalCache<String, Pattern> REGEXP_PATTERN_CACHE =
            new ThreadLocalCache<String, Pattern>() {
                @Override
                public Pattern getNewInstance(String key) {
                    return Pattern.compile(key);
                }
            };

    public RegexpSubstrFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.REGEXP_SUBSTR, context);
    }

    public @Nullable StringData eval(@Nullable StringData str, @Nullable StringData regex) {
        if (str == null || regex == null) {
            return null;
        }

        Matcher matcher;
        try {
            matcher = REGEXP_PATTERN_CACHE.get(regex.toString()).matcher(str.toString());
        } catch (PatternSyntaxException e) {
            throw new FlinkRuntimeException(e);
        }

        return matcher.find() ? BinaryStringData.fromString(matcher.group(0)) : null;
    }
}
