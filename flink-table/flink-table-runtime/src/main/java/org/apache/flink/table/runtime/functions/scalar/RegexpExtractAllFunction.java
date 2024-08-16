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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.PatternSyntaxException;

import static org.apache.flink.table.runtime.functions.SqlFunctionUtils.REGEXP_PATTERN_CACHE;

/** Implementation of {@link BuiltInFunctionDefinitions#REGEXP_EXTRACT_ALL}. */
@Internal
public class RegexpExtractAllFunction extends BuiltInScalarFunction {

    public RegexpExtractAllFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.REGEXP_EXTRACT_ALL, context);
    }

    public @Nullable ArrayData eval(@Nullable StringData str, @Nullable StringData regex) {
        return eval(str, regex, 1);
    }

    public @Nullable ArrayData eval(
            @Nullable StringData str, @Nullable StringData regex, @Nullable Number extractIndex) {
        if (str == null || regex == null || extractIndex == null) {
            return null;
        }

        Matcher matcher;
        try {
            matcher = REGEXP_PATTERN_CACHE.get(regex.toString()).matcher(str.toString());
        } catch (PatternSyntaxException e) {
            return null;
        }

        long groupIndex = extractIndex.longValue();
        if (groupIndex < 0 || matcher.groupCount() < groupIndex) {
            return null;
        }

        List<StringData> list = new ArrayList<>();
        while (matcher.find()) {
            list.add(BinaryStringData.fromString(matcher.group((int) groupIndex)));
        }

        return new GenericArrayData(list.toArray());
    }
}
