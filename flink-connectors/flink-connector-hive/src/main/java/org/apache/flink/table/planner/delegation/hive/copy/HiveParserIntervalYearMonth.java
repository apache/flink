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

package org.apache.flink.table.planner.delegation.hive.copy;

import org.apache.flink.table.planner.delegation.hive.HiveParserIntervalUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Counterpart of hive's org.apache.hadoop.hive.common.type.HiveIntervalYearMonth. */
public class HiveParserIntervalYearMonth implements Comparable<HiveParserIntervalYearMonth> {

    // years/months represented in months
    protected int totalMonths;

    protected static final int MONTHS_PER_YEAR = 12;

    public HiveParserIntervalYearMonth(int years, int months) {
        set(years, months);
    }

    public HiveParserIntervalYearMonth(int totalMonths) {
        set(totalMonths);
    }

    public int getYears() {
        return totalMonths / MONTHS_PER_YEAR;
    }

    public int getMonths() {
        return totalMonths % MONTHS_PER_YEAR;
    }

    public int getTotalMonths() {
        return totalMonths;
    }

    public void set(int years, int months) {
        this.totalMonths = months;
        this.totalMonths += years * MONTHS_PER_YEAR;
    }

    public void set(int totalMonths) {
        this.totalMonths = totalMonths;
    }

    public void set(HiveParserIntervalYearMonth other) {
        set(other.getTotalMonths());
    }

    @Override
    public int compareTo(HiveParserIntervalYearMonth other) {
        int cmp = this.getTotalMonths() - other.getTotalMonths();

        if (cmp != 0) {
            cmp = cmp > 0 ? 1 : -1;
        }
        return cmp;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof HiveParserIntervalYearMonth)) {
            return false;
        }
        return 0 == compareTo((HiveParserIntervalYearMonth) obj);
    }

    @Override
    public int hashCode() {
        return totalMonths;
    }

    @Override
    public String toString() {
        String yearMonthSignStr = totalMonths >= 0 ? "" : "-";

        return String.format(
                "%s%d-%d", yearMonthSignStr, Math.abs(getYears()), Math.abs(getMonths()));
    }

    public static HiveParserIntervalYearMonth valueOf(String strVal) {
        HiveParserIntervalYearMonth result;
        if (strVal == null) {
            throw new IllegalArgumentException("Interval year-month string was null");
        }
        Matcher patternMatcher = PATTERN_MATCHER.get();
        patternMatcher.reset(strVal);
        if (patternMatcher.matches()) {
            // Parse out the individual parts
            try {
                // Sign - whether interval is positive or negative
                int sign = 1;
                String field = patternMatcher.group(1);
                if (field != null && field.equals("-")) {
                    sign = -1;
                }
                int years =
                        sign
                                * HiveParserIntervalUtils.parseNumericValueWithRange(
                                        "year", patternMatcher.group(2), 0, Integer.MAX_VALUE);
                byte months =
                        (byte)
                                (sign
                                        * HiveParserIntervalUtils.parseNumericValueWithRange(
                                                "month", patternMatcher.group(3), 0, 11));
                result = new HiveParserIntervalYearMonth(years, months);
            } catch (Exception err) {
                throw new IllegalArgumentException(
                        "Error parsing interval year-month string: " + strVal, err);
            }
        } else {
            throw new IllegalArgumentException(
                    "Interval string does not match year-month format of 'y-m': " + strVal);
        }

        return result;
    }

    // Simple pattern: Y-M
    private static final String PARSE_PATTERN = "([+|-])?(\\d+)-(\\d+)";

    private static final ThreadLocal<Matcher> PATTERN_MATCHER =
            new ThreadLocal<Matcher>() {
                @Override
                protected Matcher initialValue() {
                    return Pattern.compile(PARSE_PATTERN).matcher("");
                }
            };
}
