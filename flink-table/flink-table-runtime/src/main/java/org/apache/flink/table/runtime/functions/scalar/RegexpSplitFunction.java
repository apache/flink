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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;

import javax.annotation.Nullable;

import java.util.regex.Pattern;

import static org.apache.flink.table.runtime.functions.SqlFunctionUtils.getRegexpPattern;

/**
 * Implementation of {@link BuiltInFunctionDefinitions#REGEXP_SPLIT}.
 *
 * <p>Splits a string by a regular expression pattern and returns an array of substrings.
 *
 * <p>Examples:
 *
 * <pre>{@code
 * REGEXP_SPLIT('Hello123World456', '[0-9]+') = ['Hello', 'World', '']
 * REGEXP_SPLIT('a,b;c', '[,;]') = ['a', 'b', 'c']
 * REGEXP_SPLIT('one  two   three', '\\s+') = ['one', 'two', 'three']
 * }</pre>
 */
@Internal
public class RegexpSplitFunction extends BuiltInScalarFunction {

    public RegexpSplitFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.REGEXP_SPLIT, context);
    }

    public @Nullable ArrayData eval(@Nullable StringData str, @Nullable StringData regex) {
        if (str == null || regex == null) {
            return null;
        }

        String regexStr = regex.toString();
        if (regexStr.isEmpty()) {
            // If regex is empty, split by each character
            String strValue = str.toString();
            StringData[] result = new StringData[strValue.length()];
            for (int i = 0; i < strValue.length(); i++) {
                result[i] = StringData.fromString(String.valueOf(strValue.charAt(i)));
            }
            return new GenericArrayData(result);
        }

        Pattern pattern = getRegexpPattern(regexStr);
        if (pattern == null) {
            // Return null for invalid regex pattern (consistent with other REGEXP_* functions)
            return null;
        }

        // Use -1 as limit to keep all trailing empty strings
        String[] splitResult = pattern.split(str.toString(), -1);
        StringData[] result = new StringData[splitResult.length];
        for (int i = 0; i < splitResult.length; i++) {
            result[i] = StringData.fromString(splitResult[i]);
        }
        return new GenericArrayData(result);
    }
}
