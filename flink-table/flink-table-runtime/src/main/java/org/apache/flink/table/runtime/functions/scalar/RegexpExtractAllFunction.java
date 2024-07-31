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
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Implementation of {@link BuiltInFunctionDefinitions#REGEXP_EXTRACT_ALL}. */
@Internal
public class RegexpExtractAllFunction extends BuiltInScalarFunction {

    private static StringData lastRegex = null;
    private static Pattern lastPattern = null;

    public RegexpExtractAllFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.REGEXP_EXTRACT_ALL, context);
    }

    public @Nullable ArrayData eval(@Nullable StringData str, @Nullable StringData regex) {
        return eval(str, regex, 0);
    }

    public @Nullable ArrayData eval(
            @Nullable StringData str, @Nullable StringData regex, @Nullable Integer extractIndex) {
        if (str == null || regex == null) {
            return null;
        }
        if (extractIndex == null) {
            extractIndex = 0;
        }

        List<StringData> list = new ArrayList<>();
        Matcher matcher = getLastMatcher(str, regex);

        checkGroupIndex(matcher.groupCount(), extractIndex);

        while (matcher.find()) {
            MatchResult matchResult = matcher.toMatchResult();
            list.add(BinaryStringData.fromString(matchResult.group(extractIndex)));
        }

        return new GenericArrayData(list.toArray());
    }

    private Matcher getLastMatcher(StringData str, StringData regex) {
        if (!regex.equals(lastRegex)) {
            try {
                lastPattern = Pattern.compile(regex.toString());
            } catch (PatternSyntaxException t) {
                throw new FlinkRuntimeException(t);
            }
            lastRegex = ((BinaryStringData) regex).copy();
        }
        return lastPattern.matcher(str.toString());
    }

    private void checkGroupIndex(int groupCount, int groupIdx) {
        if (groupIdx < 0 || groupCount < groupIdx) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Extract group index %d is out of range [0, %d].",
                            groupIdx, groupCount));
        }
    }
}
