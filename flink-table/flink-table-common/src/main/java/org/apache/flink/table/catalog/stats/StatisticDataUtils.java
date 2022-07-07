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

package org.apache.flink.table.catalog.stats;

/** Utils class for catalog column statistics. */
public class StatisticDataUtils {

    public static Double acculmulateAverage(
            Double oldAvgLength, Long oldCount, Long newLength, Long newCount) {
        if (oldAvgLength == null || oldCount == null) {
            return Double.valueOf(newLength);
        }
        if (newLength == null || newCount == null) {
            return oldAvgLength;
        }
        return oldCount + newCount == 0
                ? null
                : (oldAvgLength * oldCount + newLength * newCount) / (oldCount + newCount);
    }

    public static Long min(Long oldLength, Long newLength) {
        if (oldLength == null || newLength == null) {
            return oldLength == null ? newLength : oldLength;
        }

        return Math.min(oldLength, newLength);
    }

    public static Double min(Double oldLength, Double newLength) {
        if (oldLength == null || newLength == null) {
            return oldLength == null ? newLength : oldLength;
        }

        return Math.min(oldLength, newLength);
    }

    public static Long max(Long oldLength, Long newLength) {
        if (oldLength == null || newLength == null) {
            return oldLength == null ? newLength : oldLength;
        }

        return Math.max(oldLength, newLength);
    }

    public static Double max(Double oldLength, Double newLength) {
        if (oldLength == null || newLength == null) {
            return oldLength == null ? newLength : oldLength;
        }

        return Math.max(oldLength, newLength);
    }
}
