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

package org.apache.flink.datastream.api.extension.window.strategy;

import org.apache.flink.annotation.Experimental;

import java.time.Duration;

/** A {@link WindowStrategy} used to generate sliding TimeWindow. */
@Experimental
public class SlidingTimeWindowStrategy extends WindowStrategy {
    private final Duration windowSize;
    private final Duration windowSlideInterval;
    private final TimeType timeType;
    private final Duration allowedLateness;

    public SlidingTimeWindowStrategy(Duration windowSize, Duration windowSlideInterval) {
        this(windowSize, windowSlideInterval, TimeType.EVENT);
    }

    public SlidingTimeWindowStrategy(
            Duration windowSize, Duration windowSlideInterval, TimeType timeType) {
        this(windowSize, windowSlideInterval, timeType, Duration.ZERO);
    }

    public SlidingTimeWindowStrategy(
            Duration windowSize,
            Duration windowSlideInterval,
            TimeType timeType,
            Duration allowedLateness) {
        this.windowSize = windowSize;
        this.windowSlideInterval = windowSlideInterval;
        this.timeType = timeType;
        this.allowedLateness = allowedLateness;
    }

    public Duration getWindowSize() {
        return windowSize;
    }

    public Duration getWindowSlideInterval() {
        return windowSlideInterval;
    }

    public TimeType getTimeType() {
        return timeType;
    }

    public Duration getAllowedLateness() {
        return allowedLateness;
    }
}
