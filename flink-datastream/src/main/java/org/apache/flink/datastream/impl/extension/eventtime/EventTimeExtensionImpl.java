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

package org.apache.flink.datastream.impl.extension.eventtime;

import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.function.OneInputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.function.TwoInputBroadcastEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.function.TwoInputNonBroadcastEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.function.TwoOutputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeWatermarkStrategy;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.functions.EventTimeWrappedOneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.functions.EventTimeWrappedTwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.functions.EventTimeWrappedTwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.functions.EventTimeWrappedTwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.functions.ExtractEventTimeProcessFunction;

/** The implementation of {@link EventTimeExtension}. */
public class EventTimeExtensionImpl {

    // ============= Extract Event Time Process Function =============

    /**
     * Build an {@link ExtractEventTimeProcessFunction} to extract event time according to {@link
     * EventTimeWatermarkStrategy}.
     */
    public static <T> OneInputStreamProcessFunction<T, T> buildAsProcessFunction(
            EventTimeWatermarkStrategy<T> strategy) {
        return new ExtractEventTimeProcessFunction<>(strategy);
    }

    // ============= Wrap Event Time Process Function =============

    public static <IN, OUT> OneInputStreamProcessFunction<IN, OUT> wrapProcessFunction(
            OneInputEventTimeStreamProcessFunction<IN, OUT> processFunction) {
        return new EventTimeWrappedOneInputStreamProcessFunction<>(processFunction);
    }

    public static <IN, OUT1, OUT2>
            TwoOutputStreamProcessFunction<IN, OUT1, OUT2> wrapProcessFunction(
                    TwoOutputEventTimeStreamProcessFunction<IN, OUT1, OUT2> processFunction) {
        return new EventTimeWrappedTwoOutputStreamProcessFunction<>(processFunction);
    }

    public static <IN1, IN2, OUT>
            TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> wrapProcessFunction(
                    TwoInputNonBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT>
                            processFunction) {
        return new EventTimeWrappedTwoInputNonBroadcastStreamProcessFunction<>(processFunction);
    }

    public static <IN1, IN2, OUT>
            TwoInputBroadcastStreamProcessFunction<IN1, IN2, OUT> wrapProcessFunction(
                    TwoInputBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT>
                            processFunction) {
        return new EventTimeWrappedTwoInputBroadcastStreamProcessFunction<>(processFunction);
    }

    // ============= Other Utils =============
    public static boolean isEventTimeExtensionWatermark(Watermark watermark) {
        return EventTimeExtension.isEventTimeWatermark(watermark)
                || EventTimeExtension.isIdleStatusWatermark(watermark);
    }
}
