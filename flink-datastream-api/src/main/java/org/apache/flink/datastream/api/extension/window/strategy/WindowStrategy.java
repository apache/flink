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

import java.io.Serializable;
import java.time.Duration;

/**
 * This class describes what kind of Windows to use, including strategies for dividing, triggering,
 * and clearing Windows.
 *
 * <p>We currently provide three built-in window types: Global Window, Time Window, and Session
 * Window.
 *
 * <ul>
 *   <li>Global Window: All the data is in a single window. There is only one Global Window, and all
 *       data is assigned to this single window. Global Window are suitable for bounded stream
 *       scenarios and can be used in GlobalStream, KeyedStream, and NonKeyedStream.
 *   <li>Time Window: Data within a specific time period is assigned to a single window. Time
 *       Windows are divided into multiple windows based on time ranges, and data is assigned to the
 *       corresponding window based on its timestamp. We support two types of time windows: tumbling
 *       windows and sliding windows, and the tumbling windows cannot overlap. The time semantics
 *       within the windows can be divided into event time and processing time. Time Window can be
 *       used in GlobalStream and KeyedStream.
 *   <li>Session Window: Consecutive data is assigned to a single window. Session windows are a
 *       special type of time window and are divided into multiple windows based on time ranges.
 *       When data arrives, it is first assigned to the corresponding window based on its timestamp,
 *       and then existing windows are merged as much as possible. Session Window can be used in
 *       GlobalStream and KeyedStream.
 * </ul>
 */
@Experimental
public class WindowStrategy implements Serializable {

    public static final TimeType PROCESSING_TIME = TimeType.PROCESSING;
    public static final TimeType EVENT_TIME = TimeType.EVENT;

    /** The types of time used in window operations. */
    @Experimental
    public enum TimeType {
        PROCESSING,
        EVENT
    }

    // ============== global window ================

    /**
     * Creates a global window strategy. Note that the global window can be used in both
     * GlobalStream, KeyedStream, NonKeyedStream.
     *
     * @return A global window strategy.
     */
    public static WindowStrategy global() {
        return new GlobalWindowStrategy();
    }

    // ============== tumbling time window ================

    /**
     * Create a tumbling time window strategy with the event time default time type. Note that
     * tumbling time windows can be used in KeyedStream and GlobalStream. If tumbling time window is
     * used in a GlobalStream, it will convert the GlobalStream into a KeyedStream with a Key of
     * zero, and then use the converted KeyedStream to execute the window.
     *
     * @param windowSize the size of Window.
     * @return A tumbling time window strategy.
     */
    public static WindowStrategy tumbling(Duration windowSize) {
        return new TumblingTimeWindowStrategy(windowSize);
    }

    /**
     * Create a tumbling time window strategy. Note that tumbling time windows can be used in
     * KeyedStream and GlobalStream. If tumbling time window is used in a GlobalStream, it will
     * convert the GlobalStream into a KeyedStream with a Key of zero, and then use the converted
     * KeyedStream to execute the window.
     *
     * @param windowSize the size of Window.
     * @param timeType the time type of Window.
     * @return A tumbling time window strategy.
     */
    public static WindowStrategy tumbling(Duration windowSize, TimeType timeType) {
        return new TumblingTimeWindowStrategy(windowSize, timeType);
    }

    /**
     * Create a tumbling time window strategy. Note that tumbling time windows can be used in
     * KeyedStream and GlobalStream. If tumbling time window is used in a GlobalStream, it will
     * convert the GlobalStream into a KeyedStream with a Key of zero, and then use the converted
     * KeyedStream to execute the window.
     *
     * @param windowSize the size of Window.
     * @param timeType the time type of Window.
     * @param allowedLateness the allowed lateness of Window.
     * @return A tumbling time window strategy.
     */
    public static WindowStrategy tumbling(
            Duration windowSize, TimeType timeType, Duration allowedLateness) {
        return new TumblingTimeWindowStrategy(windowSize, timeType, allowedLateness);
    }

    // ============== sliding time window ================

    /**
     * Create a sliding time window strategy with the event time default time type. Note that
     * sliding time windows can be used in KeyedStream and GlobalStream. If sliding time window is
     * used in a GlobalStream, it will convert the GlobalStream into a KeyedStream with a Key of
     * zero, and then use the converted KeyedStream to execute the window.
     *
     * @param windowSize the size of Window.
     * @param windowSlideInterval the slide interval of Window.
     * @return A sliding time window strategy.
     */
    public static WindowStrategy sliding(Duration windowSize, Duration windowSlideInterval) {
        return new SlidingTimeWindowStrategy(windowSize, windowSlideInterval);
    }

    /**
     * Create a sliding time window strategy. Note that sliding time windows can be used in
     * KeyedStream and GlobalStream. If sliding time window is used in a GlobalStream, it will
     * convert the GlobalStream into a KeyedStream with a Key of zero, and then use the converted
     * KeyedStream to execute the window.
     *
     * @param windowSize the size of Window.
     * @param windowSlideInterval the slide interval of Window.
     * @param timeType the time type of Window.
     * @return A sliding time window strategy.
     */
    public static WindowStrategy sliding(
            Duration windowSize, Duration windowSlideInterval, TimeType timeType) {
        return new SlidingTimeWindowStrategy(windowSize, windowSlideInterval, timeType);
    }

    /**
     * Create a sliding time window strategy. Note that sliding time windows can be used in
     * KeyedStream and GlobalStream. If sliding time window is used in a GlobalStream, it will
     * convert the GlobalStream into a KeyedStream with a Key of zero, and then use the converted
     * KeyedStream to execute the window.
     *
     * @param windowSize the size of Window.
     * @param windowSlideInterval the slide interval of Window.
     * @param timeType the time type of Window.
     * @param allowedLateness the allowed lateness of Window.
     * @return A sliding time window strategy.
     */
    public static WindowStrategy sliding(
            Duration windowSize,
            Duration windowSlideInterval,
            TimeType timeType,
            Duration allowedLateness) {
        return new SlidingTimeWindowStrategy(
                windowSize, windowSlideInterval, timeType, allowedLateness);
    }

    // ============== session window ================

    /**
     * Create a session time window strategy with the event time default time type. Note that
     * session time windows can be used in KeyedStream and GlobalStream. If session time window is
     * used in a GlobalStream, it will convert the GlobalStream into a KeyedStream with a Key of
     * zero, and then use the converted KeyedStream to execute the window.
     *
     * @param sessionGap the timeout of session.
     * @return A session window strategy.
     */
    public static WindowStrategy session(Duration sessionGap) {
        return new SessionWindowStrategy(sessionGap);
    }

    /**
     * Create a session time window strategy. Note that session time windows can be used in
     * KeyedStream and GlobalStream. If session time window is used in a GlobalStream, it will
     * convert the GlobalStream into a KeyedStream with a Key of zero, and then use the converted
     * KeyedStream to execute the window.
     *
     * @param sessionGap the timeout of session.
     * @param timeType the time type of Window.
     * @return A session window strategy.
     */
    public static WindowStrategy session(Duration sessionGap, TimeType timeType) {
        return new SessionWindowStrategy(sessionGap, timeType);
    }
}
