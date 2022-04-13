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

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;

import javax.annotation.Nullable;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/** Implementation of {@link BuiltInFunctionDefinitions#DATE_ADD}. */
public class DateAddFunction extends BuiltInScalarFunction {

    /** The SimpleDateFormat string for ISO dates, "yyyy-MM-dd". */
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd";

    /** The SimpleDateFormat string for ISO times, "HH:mm:ss". */
    private static final String TIME_FORMAT_STRING = "HH:mm:ss";

    /** The SimpleDateFormat string for ISO timestamps, "yyyy-MM-dd HH:mm:ss". */
    private static final String TIMESTAMP_FORMAT_STRING =
            DATE_FORMAT_STRING + " " + TIME_FORMAT_STRING;

    public DateAddFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.DATE_ADD, context);
    }

    public @Nullable StringData eval(StringData startDate, int numberOfDays) {
        final String dateStr = startDate.toString();
        long startMillSecond;
        try {
            startMillSecond = new SimpleDateFormat(DATE_FORMAT_STRING).parse(dateStr).getTime();
        } catch (ParseException e) {
            try {
                startMillSecond =
                        new SimpleDateFormat(TIMESTAMP_FORMAT_STRING).parse(dateStr).getTime();
            } catch (ParseException parseException) {
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported datetime format '%s', "
                                        + "please use '%s' or '%s' instead.",
                                dateStr, DATE_FORMAT_STRING, TIMESTAMP_FORMAT_STRING));
            }
        }
        final Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(startMillSecond);
        calendar.add(Calendar.DAY_OF_MONTH, numberOfDays);
        return StringData.fromString(new SimpleDateFormat(DATE_FORMAT_STRING)
                .format(new Date(calendar.getTimeInMillis())));
    }

}
