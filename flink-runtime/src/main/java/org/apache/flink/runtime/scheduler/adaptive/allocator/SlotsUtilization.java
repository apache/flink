/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.util.Preconditions;

import java.util.Objects;

/** Help class to represent the slots utilization info. */
class SlotsUtilization {

    private final int total;
    private final int reserved;

    SlotsUtilization(int total, int reserved) {
        Preconditions.checkArgument(
                total >= reserved, "The total value must be >= reserved value.");
        Preconditions.checkArgument(reserved >= 0, "The reserved number must not be negative.");
        this.total = total;
        this.reserved = reserved;
    }

    SlotsUtilization incReserved(int inc) {
        Preconditions.checkArgument(inc > 0, "The increment number must be greater than zero.");
        Preconditions.checkArgument(
                reserved + inc <= total,
                "The increment result must be equal to or less than the total value.");
        return new SlotsUtilization(total, reserved + inc);
    }

    double getUtilization() {
        if (total == 0 && reserved == 0) {
            return 1.0;
        }
        return ((double) reserved) / total;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SlotsUtilization that = (SlotsUtilization) o;
        return total == that.total && reserved == that.reserved;
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, reserved);
    }
}
