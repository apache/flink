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
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.utils.DateTimeUtils;

import javax.annotation.Nullable;

import static org.apache.flink.table.utils.DateTimeUtils.parseTimestampData;

/**
 * Implementation of {@link BuiltInFunctionDefinitions#TO_TIMESTAMP}.
 *
 * <p>Supported function signatures:
 *
 * <ul>
 *   <li>{@code TO_TIMESTAMP(string)} -> TIMESTAMP(3) <br>
 *       Parses string timestamp using default format 'yyyy-MM-dd HH:mm:ss'
 *   <li>{@code TO_TIMESTAMP(string, format)} -> TIMESTAMP(precision) <br>
 *       Parses string timestamp using the given format. Output precision is inferred from the
 *       format pattern's trailing 'S' count, with a minimum of 3 for backward compatibility.
 * </ul>
 */
@Internal
public class ToTimestampFunction extends BuiltInScalarFunction {

    public ToTimestampFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.TO_TIMESTAMP, context);
    }

    public @Nullable TimestampData eval(@Nullable StringData timestamp) {
        if (timestamp == null) {
            return null;
        }

        return parseTimestampData(timestamp.toString());
    }

    public @Nullable TimestampData eval(
            @Nullable StringData timestamp, @Nullable StringData format) {
        if (timestamp == null || format == null) {
            return null;
        }

        String formatStr = format.toString();
        return parseTimestampData(
                timestamp.toString(), formatStr, DateTimeUtils.precisionFromFormat(formatStr));
    }
}
