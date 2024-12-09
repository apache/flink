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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.utils.DateTimeUtils;

/**
 * Implementation of {@link BuiltInFunctionDefinitions#TO_TIMESTAMP_LTZ}.
 *
 * <p>Supported function signatures: TO_TIMESTAMP_LTZ(numeric, precision) ->
 * TIMESTAMP_LTZ(precision) TO_TIMESTAMP_LTZ(string) -> TIMESTAMP_LTZ(3) TO_TIMESTAMP_LTZ(string,
 * format) -> TIMESTAMP_LTZ(3) TO_TIMESTAMP_LTZ(string, format, timezone) -> TIMESTAMP_LTZ(3)
 * TO_TIMESTAMP_LTZ(numeric) -> TIMESTAMP_LTZ(3)
 */
@Internal
public class ToTimestampLtzFunction extends BuiltInScalarFunction {

    public ToTimestampLtzFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.TO_TIMESTAMP_LTZ, context);
    }

    public TimestampData eval(Integer epoch, Integer precision) {
        if (epoch == null || precision == null) {
            return null;
        }

        return DateTimeUtils.toTimestampData(epoch, precision);
    }

    public TimestampData eval(Long epoch, Integer precision) {
        if (epoch == null || precision == null) {
            return null;
        }

        return DateTimeUtils.toTimestampData(epoch, precision);
    }

    public TimestampData eval(Double epoch, Integer precision) {
        if (epoch == null || precision == null) {
            return null;
        }

        return DateTimeUtils.toTimestampData(epoch, precision);
    }

    public TimestampData eval(Float value, Integer precision) {
        if (value == null || precision == null) {
            return null;
        }
        return DateTimeUtils.toTimestampData(value.longValue(), precision);
    }

    public TimestampData eval(Byte value, Integer precision) {
        if (value == null || precision == null) {
            return null;
        }
        return DateTimeUtils.toTimestampData(value.longValue(), precision);
    }

    public TimestampData eval(DecimalData epoch, Integer precision) {
        if (epoch == null || precision == null) {
            return null;
        }

        return DateTimeUtils.toTimestampData(epoch, precision);
    }

    public TimestampData eval(Integer epoch) {
        if (epoch == null) {
            return null;
        }

        return eval(epoch, 3);
    }

    public TimestampData eval(Long epoch) {
        if (epoch == null) {
            return null;
        }

        return eval(epoch, 3);
    }

    public TimestampData eval(Float epoch) {
        if (epoch == null) {
            return null;
        }

        return eval(epoch, 3);
    }

    public TimestampData eval(Byte epoch) {
        if (epoch == null) {
            return null;
        }

        return eval(epoch, 3);
    }

    public TimestampData eval(Double epoch) {
        if (epoch == null) {
            return null;
        }

        return eval(epoch, 3);
    }

    public TimestampData eval(DecimalData epoch) {
        if (epoch == null) {
            return null;
        }

        return eval(epoch, 3);
    }

    // Parse timestamp with default format
    public TimestampData eval(StringData timestamp) {
        if (timestamp == null) {
            return null;
        }

        return DateTimeUtils.parseTimestampData(timestamp.toString());
    }

    // Parse timestamp with custom format
    public TimestampData eval(StringData timestamp, StringData format) {
        if (timestamp == null || format == null) {
            return null;
        }

        return DateTimeUtils.parseTimestampData(timestamp.toString(), format.toString());
    }

    // Parse timestamp with format and timezone
    public TimestampData eval(StringData timestamp, StringData format, StringData timezone) {
        if (timestamp == null || format == null || timezone == null) {
            return null;
        }

        return DateTimeUtils.toTimestampData(
                timestamp.toString(), format.toString(), timezone.toString());
    }
}
