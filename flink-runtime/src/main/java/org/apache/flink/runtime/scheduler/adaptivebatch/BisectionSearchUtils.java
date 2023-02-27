/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.adaptivebatch;

import java.util.function.Function;

/** Utility class for bisection search. */
class BisectionSearchUtils {

    public static long findMinLegalValue(
            Function<Long, Boolean> legalChecker, long low, long high) {
        if (!legalChecker.apply(high)) {
            return -1;
        }
        while (low <= high) {
            long mid = (low + high) / 2;
            if (legalChecker.apply(mid)) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return high + 1;
    }

    public static long findMaxLegalValue(
            Function<Long, Boolean> legalChecker, long low, long high) {
        if (!legalChecker.apply(low)) {
            return -1;
        }
        while (low <= high) {
            long mid = (low + high) / 2;
            if (legalChecker.apply(mid)) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return low - 1;
    }
}
