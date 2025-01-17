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

package org.apache.flink.datastream.api.extension.eventtime;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.watermark.BoolWatermarkDeclaration;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclarations;
import org.apache.flink.datastream.api.extension.eventtime.function.EventTimeProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.function.OneInputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.function.TwoInputBroadcastEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.function.TwoInputNonBroadcastEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.function.TwoOutputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeExtractor;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeWatermarkGeneratorBuilder;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.ProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;

/**
 * The entry point for the event-time extension, which provides the following functionality:
 *
 * <ul>
 *   <li>defines the event-time watermark and idle status watermark. If you use the {@link
 *       EventTimeWatermarkGeneratorBuilder} below, then you don't need to declare these watermarks
 *       manually in your application; otherwise you need to declare them in your own {@link
 *       ProcessFunction}.
 *   <li>provides the {@link EventTimeWatermarkGeneratorBuilder} to facilitate the generation of
 *       event time watermarks. An example of using {@link EventTimeWatermarkGeneratorBuilder} is as
 *       follows:
 *       <pre>{@code
 * OneInputStreamProcessFunction<POJO, POJO> watermarkGeneratorProcessFunction
 *       = EventTimeExtension
 *       .newWatermarkGeneratorBuilder(POJO::getEventTime)
 *       .periodicWatermark()
 *       .buildAsProcessFunction();
 * source.process(watermarkGeneratorProcessFunction)
 *       .process(...)
 * }</pre>
 *   <li>provides a tool to encapsulate a user-defined {@link EventTimeProcessFunction} to provide
 *       the relevant components of the event-time extension.
 *       <pre>{@code
 * stream.process(
 *          EventTimeExtension.wrapProcessFunction(
 *              new CustomEventTimeProcessFunction()
 *          )
 *       )
 *       .process(...)
 * }</pre>
 * </ul>
 */
@Experimental
public class EventTimeExtension {

    // =============== Event Time related Watermark Declarations ===============

    /**
     * Definition of EventTimeWatermark. The EventTimeWatermark represents a specific timestamp,
     * signifying the passage of time. Once a process function receives an EventTimeWatermark, it
     * will no longer receive events with a timestamp earlier than that watermark.
     */
    public static final LongWatermarkDeclaration EVENT_TIME_WATERMARK_DECLARATION =
            WatermarkDeclarations.newBuilder("BUILTIN_API_EVENT_TIME")
                    .typeLong()
                    .combineFunctionMin()
                    .combineWaitForAllChannels(true)
                    .defaultHandlingStrategyForward()
                    .build();

    /**
     * Definition of IdleStatusWatermark. The IdleStatusWatermark indicates that a particular input
     * is in an idle state. When a ProcessFunction receives an IdleStatusWatermark from an input, it
     * should ignore that input when combining EventTimeWatermarks.
     */
    public static final BoolWatermarkDeclaration IDLE_STATUS_WATERMARK_DECLARATION =
            WatermarkDeclarations.newBuilder("BUILTIN_API_EVENT_TIME_IDLE")
                    .typeBool()
                    .combineFunctionAND()
                    .combineWaitForAllChannels(true)
                    .defaultHandlingStrategyForward()
                    .build();

    /**
     * Determine if the received watermark is an EventTimeWatermark.
     *
     * @param watermark The watermark to be checked.
     * @return true if the watermark is an EventTimeWatermark; false otherwise.
     */
    public static boolean isEventTimeWatermark(Watermark watermark) {
        return isEventTimeWatermark(watermark.getIdentifier());
    }

    /**
     * Determine if the received watermark is an EventTimeWatermark by watermark identifier.
     *
     * @param watermarkIdentifier The identifier of the watermark to be checked.
     * @return true if the watermark is an EventTimeWatermark; false otherwise.
     */
    public static boolean isEventTimeWatermark(String watermarkIdentifier) {
        return watermarkIdentifier.equals(EVENT_TIME_WATERMARK_DECLARATION.getIdentifier());
    }

    /**
     * Determine if the received watermark is an IdleStatusWatermark.
     *
     * @param watermark The watermark to be checked.
     * @return true if the watermark is an IdleStatusWatermark; false otherwise.
     */
    public static boolean isIdleStatusWatermark(Watermark watermark) {
        return isIdleStatusWatermark(watermark.getIdentifier());
    }

    /**
     * Determine if the received watermark is an IdleStatusWatermark by watermark identifier.
     *
     * @param watermarkIdentifier The identifier of the watermark to be checked.
     * @return true if the watermark is an IdleStatusWatermark; false otherwise.
     */
    public static boolean isIdleStatusWatermark(String watermarkIdentifier) {
        return watermarkIdentifier.equals(IDLE_STATUS_WATERMARK_DECLARATION.getIdentifier());
    }

    // ======== EventTimeWatermarkGeneratorBuilder to generate event time watermarks =========

    /**
     * Create an instance of {@link EventTimeWatermarkGeneratorBuilder}, which contains a {@code
     * EventTimeExtractor}.
     *
     * @param eventTimeExtractor An instance of {@code EventTimeExtractor} used to extract event
     *     time information from data records.
     * @param <T> The type of data records.
     * @return An instance of {@code EventTimeWatermarkGeneratorBuilder} containing the specified
     *     event time extractor.
     */
    public static <T> EventTimeWatermarkGeneratorBuilder<T> newWatermarkGeneratorBuilder(
            EventTimeExtractor<T> eventTimeExtractor) {
        return new EventTimeWatermarkGeneratorBuilder<>(eventTimeExtractor);
    }

    // ======== Wrap user-defined {@link EventTimeProcessFunction} =========

    /**
     * Wrap the user-defined {@link OneInputEventTimeStreamProcessFunction}, which will provide
     * related components such as {@link EventTimeManager} and declare the necessary built-in state
     * required for the Timer, etc. Note that registering event timers of {@link
     * EventTimeProcessFunction} can only be used with {@link KeyedPartitionStream}.
     *
     * @param processFunction The user-defined {@link OneInputEventTimeStreamProcessFunction} that
     *     needs to be wrapped.
     * @return The wrapped {@link OneInputStreamProcessFunction}.
     */
    public static <IN, OUT> OneInputStreamProcessFunction<IN, OUT> wrapProcessFunction(
            OneInputEventTimeStreamProcessFunction<IN, OUT> processFunction) {
        try {
            return (OneInputStreamProcessFunction<IN, OUT>)
                    getEventTimeExtensionImplClass()
                            .getMethod(
                                    "wrapProcessFunction",
                                    OneInputEventTimeStreamProcessFunction.class)
                            .invoke(null, processFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Wrap the user-defined {@link TwoOutputStreamProcessFunction}, which will provide related
     * components such as {@link EventTimeManager} and declare the necessary built-in state required
     * for the Timer, etc. Note that registering event timers of {@link EventTimeProcessFunction}
     * can only be used with {@link KeyedPartitionStream}.
     *
     * @param processFunction The user-defined {@link TwoOutputEventTimeStreamProcessFunction} that
     *     needs to be wrapped.
     * @return The wrapped {@link TwoOutputStreamProcessFunction}.
     */
    public static <IN, OUT1, OUT2>
            TwoOutputStreamProcessFunction<IN, OUT1, OUT2> wrapProcessFunction(
                    TwoOutputEventTimeStreamProcessFunction<IN, OUT1, OUT2> processFunction) {
        try {
            return (TwoOutputStreamProcessFunction<IN, OUT1, OUT2>)
                    getEventTimeExtensionImplClass()
                            .getMethod(
                                    "wrapProcessFunction",
                                    TwoOutputEventTimeStreamProcessFunction.class)
                            .invoke(null, processFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Wrap the user-defined {@link TwoInputNonBroadcastEventTimeStreamProcessFunction}, which will
     * provide related components such as {@link EventTimeManager} and declare the necessary
     * built-in state required for the Timer, etc. Note that registering event timers of {@link
     * EventTimeProcessFunction} can only be used with {@link KeyedPartitionStream}.
     *
     * @param processFunction The user-defined {@link
     *     TwoInputNonBroadcastEventTimeStreamProcessFunction} that needs to be wrapped.
     * @return The wrapped {@link TwoInputNonBroadcastStreamProcessFunction}.
     */
    public static <IN1, IN2, OUT>
            TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> wrapProcessFunction(
                    TwoInputNonBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT>
                            processFunction) {
        try {
            return (TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT>)
                    getEventTimeExtensionImplClass()
                            .getMethod(
                                    "wrapProcessFunction",
                                    TwoInputNonBroadcastEventTimeStreamProcessFunction.class)
                            .invoke(null, processFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Wrap the user-defined {@link TwoInputBroadcastEventTimeStreamProcessFunction}, which will
     * provide related components such as {@link EventTimeManager} and declare the necessary
     * built-in state required for the Timer, etc. Note that registering event timers of {@link
     * EventTimeProcessFunction} can only be used with {@link KeyedPartitionStream}.
     *
     * @param processFunction The user-defined {@link
     *     TwoInputBroadcastEventTimeStreamProcessFunction} that needs to be wrapped.
     * @return The wrapped {@link TwoInputBroadcastStreamProcessFunction}.
     */
    public static <IN1, IN2, OUT>
            TwoInputBroadcastStreamProcessFunction<IN1, IN2, OUT> wrapProcessFunction(
                    TwoInputBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT>
                            processFunction) {
        try {
            return (TwoInputBroadcastStreamProcessFunction<IN1, IN2, OUT>)
                    getEventTimeExtensionImplClass()
                            .getMethod(
                                    "wrapProcessFunction",
                                    TwoInputBroadcastEventTimeStreamProcessFunction.class)
                            .invoke(null, processFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Get the implementation class of EventTimeExtension. */
    private static Class<?> getEventTimeExtensionImplClass() {
        try {
            return Class.forName(
                    "org.apache.flink.datastream.impl.extension.eventtime.EventTimeExtensionImpl");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Please ensure that flink-datastream in your class path");
        }
    }
}
