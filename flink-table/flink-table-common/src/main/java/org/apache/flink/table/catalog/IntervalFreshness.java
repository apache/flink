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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Objects;

/**
 * The {@link IntervalFreshness} represents freshness definition of {@link
 * CatalogMaterializedTable}. It encapsulates the string interval value along with time unit,
 * allowing for flexible representation of different freshness type. Moreover, it can provide
 * detailed raw information for some specific operations.
 */
@PublicEvolving
public class IntervalFreshness {

    private final String interval;
    private final TimeUnit timeUnit;

    private IntervalFreshness(String interval, TimeUnit timeUnit) {
        this.interval = interval;
        this.timeUnit = timeUnit;
    }

    public static IntervalFreshness of(String interval, TimeUnit timeUnit) {
        return new IntervalFreshness(interval, timeUnit);
    }

    public static IntervalFreshness ofSecond(String interval) {
        return new IntervalFreshness(interval, TimeUnit.SECOND);
    }

    public static IntervalFreshness ofMinute(String interval) {
        return new IntervalFreshness(interval, TimeUnit.MINUTE);
    }

    public static IntervalFreshness ofHour(String interval) {
        return new IntervalFreshness(interval, TimeUnit.HOUR);
    }

    public static IntervalFreshness ofDay(String interval) {
        return new IntervalFreshness(interval, TimeUnit.DAY);
    }

    public String getInterval() {
        return interval;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IntervalFreshness that = (IntervalFreshness) o;
        return Objects.equals(interval, that.interval) && timeUnit == that.timeUnit;
    }

    @Override
    public String toString() {
        return "INTERVAL '" + interval + "' " + timeUnit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(interval, timeUnit);
    }

    // --------------------------------------------------------------------------------------------
    // TimeUnit enums
    // --------------------------------------------------------------------------------------------

    /** An enumeration of time unit representing the unit of interval freshness. */
    @PublicEvolving
    public enum TimeUnit {
        SECOND,
        MINUTE,
        HOUR,
        DAY
    }
}
