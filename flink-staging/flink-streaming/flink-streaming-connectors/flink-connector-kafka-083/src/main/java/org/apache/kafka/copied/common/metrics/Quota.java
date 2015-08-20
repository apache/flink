/**
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
package org.apache.kafka.copied.common.metrics;

/**
 * An upper or lower bound for metrics
 */
public final class Quota {

    private final boolean upper;
    private final double bound;

    public Quota(double bound, boolean upper) {
        this.bound = bound;
        this.upper = upper;
    }

    public static Quota lessThan(double upperBound) {
        return new Quota(upperBound, true);
    }

    public static Quota moreThan(double lowerBound) {
        return new Quota(lowerBound, false);
    }

    public boolean isUpperBound() {
        return this.upper;
    }

    public double bound() {
        return this.bound;
    }

    public boolean acceptable(double value) {
        return (upper && value <= bound) || (!upper && value >= bound);
    }

}
