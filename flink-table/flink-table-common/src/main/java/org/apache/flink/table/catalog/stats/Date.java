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

import org.apache.flink.annotation.PublicEvolving;

import java.util.Objects;

/** Class representing a date value in statistics. */
@PublicEvolving
public class Date {
    private final long daysSinceEpoch;

    public Date(long daysSinceEpoch) {
        this.daysSinceEpoch = daysSinceEpoch;
    }

    public long getDaysSinceEpoch() {
        return daysSinceEpoch;
    }

    public Date copy() {
        return new Date(daysSinceEpoch);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Date date = (Date) o;
        return daysSinceEpoch == date.daysSinceEpoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(daysSinceEpoch);
    }

    @Override
    public String toString() {
        return "Date{" + "daysSinceEpoch=" + daysSinceEpoch + '}';
    }
}
