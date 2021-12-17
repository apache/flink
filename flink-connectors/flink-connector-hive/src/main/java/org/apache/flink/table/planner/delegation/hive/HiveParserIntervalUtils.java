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

package org.apache.flink.table.planner.delegation.hive;

import java.math.BigDecimal;

/** Util class for hive interval values. */
public class HiveParserIntervalUtils {

    public static final int NANOS_PER_SEC = 1000000000;
    public static final BigDecimal MAX_INT_BD = new BigDecimal(Integer.MAX_VALUE);
    public static final BigDecimal NANOS_PER_SEC_BD = new BigDecimal(NANOS_PER_SEC);

    private HiveParserIntervalUtils() {}

    public static int parseNumericValueWithRange(
            String fieldName, String strVal, int minValue, int maxValue)
            throws IllegalArgumentException {
        int result = 0;
        if (strVal != null) {
            result = Integer.parseInt(strVal);
            if (result < minValue || result > maxValue) {
                throw new IllegalArgumentException(
                        String.format(
                                "%s value %d outside range [%d, %d]",
                                fieldName, result, minValue, maxValue));
            }
        }
        return result;
    }
}
