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

/** Implementation of {@link BuiltInFunctionDefinitions#SPLIT}. */
@Internal
public class SplitFunction extends BuiltInScalarFunction {
    public SplitFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.SPLIT, context);
    }

    public @Nullable ArrayData eval(@Nullable StringData string, @Nullable StringData delimiter) {
        try {
            if (string == null || delimiter == null) {
                return null;
            }
            String string1 = string.toString();
            String delimiter1 = delimiter.toString();
            List<StringData> resultList = new ArrayList<>();

            if (delimiter1.equals("")) {
                for (char c : string1.toCharArray()) {
                    resultList.add(BinaryStringData.fromString(String.valueOf(c)));
                }
            } else {
                int start = 0;
                int end = string1.indexOf(delimiter1);
                while (end != -1) {
                    String substring = string1.substring(start, end);
                    resultList.add(
                            BinaryStringData.fromString(
                                    substring.isEmpty()
                                            ? ""
                                            : substring)); // Added this check to handle consecutive
                    // delimiters
                    start = end + delimiter1.length();
                    end = string1.indexOf(delimiter1, start);
                }
                String remaining = string1.substring(start);
                resultList.add(
                        BinaryStringData.fromString(
                                remaining.isEmpty()
                                        ? ""
                                        : remaining)); // Added this check to handle delimiter at
                // the end
            }
            return new GenericArrayData(resultList.toArray());
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }
}
