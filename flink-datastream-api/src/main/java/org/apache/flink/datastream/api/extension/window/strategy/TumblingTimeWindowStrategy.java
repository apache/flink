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

/** A {@link WindowStrategy} used to generate tumbling TimeWindow. */
@Experimental
public class TumblingTimeWindowStrategy extends WindowStrategy {
    private final Duration windowSize;
    private final TimeType timeType;
    private final Duration allowedLateness;

    public TumblingTimeWindowStrategy(Duration windowSize) {
        this(windowSize, TimeType.EVENT);
    }

    public TumblingTimeWindowStrategy(Duration windowSize, TimeType timeType) {
        this(windowSize, timeType, Duration.ZERO);
    }

    public TumblingTimeWindowStrategy(
            Duration windowSize, TimeType timeType, Duration allowedLateness) {
        this.windowSize = windowSize;
        this.timeType = timeType;
        this.allowedLateness = allowedLateness;
    }

    public Duration getWindowSize() {
        return windowSize;
    }

    public TimeType getTimeType() {
        return timeType;
    }

    public Duration getAllowedLateness() {
        return allowedLateness;
    }
}
