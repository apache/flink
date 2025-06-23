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

package org.apache.flink.datastream.api.extension.eventtime.strategy;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;

import java.time.Duration;

/**
 * A utility class for constructing a processing function that extracts event time and generates
 * event time watermarks in the {@link EventTimeExtension}.
 */
@Experimental
public class EventTimeWatermarkGeneratorBuilder<T> {
    // how to extract event time from event
    private EventTimeExtractor<T> eventTimeExtractor;

    // what frequency to generate event time watermark
    private EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode generateMode =
            EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PERIODIC;

    // if not set, it will default to the value of the "pipeline.auto-watermark-interval"
    // configuration.
    private Duration periodicWatermarkInterval = Duration.ZERO;

    // if set to zero, it will not generate idle status watermark
    private Duration idleTimeout = Duration.ZERO;

    // max out-of-order time
    private Duration maxOutOfOrderTime = Duration.ZERO;

    // =========  how to extract event times from events =========

    public EventTimeWatermarkGeneratorBuilder(EventTimeExtractor<T> eventTimeExtractor) {
        this.eventTimeExtractor = eventTimeExtractor;
    }

    // =========  generate the event time watermark with what value =========

    public EventTimeWatermarkGeneratorBuilder<T> withIdleness(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
        return this;
    }

    public EventTimeWatermarkGeneratorBuilder<T> withMaxOutOfOrderTime(Duration maxOutOfOrderTime) {
        this.maxOutOfOrderTime = maxOutOfOrderTime;
        return this;
    }

    // =========  when to generate event time watermark =========

    public EventTimeWatermarkGeneratorBuilder<T> noWatermark() {
        this.generateMode = EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.NO_WATERMARK;
        return this;
    }

    /**
     * The periodic watermark interval will be set to the value specified by
     * PipelineOptions#AUTO_WATERMARK_INTERVAL.
     */
    public EventTimeWatermarkGeneratorBuilder<T> periodicWatermark() {
        this.generateMode = EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PERIODIC;
        return this;
    }

    public EventTimeWatermarkGeneratorBuilder<T> periodicWatermark(
            Duration periodicWatermarkInterval) {
        this.generateMode = EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PERIODIC;
        this.periodicWatermarkInterval = periodicWatermarkInterval;
        return this;
    }

    public EventTimeWatermarkGeneratorBuilder<T> perEventWatermark() {
        this.generateMode = EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PER_EVENT;
        return this;
    }

    // =========  build the watermark generator as process function =========

    public OneInputStreamProcessFunction<T, T> buildAsProcessFunction() {
        EventTimeWatermarkStrategy<T> watermarkStrategy =
                new EventTimeWatermarkStrategy<>(
                        this.eventTimeExtractor,
                        this.generateMode,
                        this.periodicWatermarkInterval,
                        this.idleTimeout,
                        this.maxOutOfOrderTime);

        try {
            return (OneInputStreamProcessFunction<T, T>)
                    getEventTimeExtensionImplClass()
                            .getMethod("buildAsProcessFunction", EventTimeWatermarkStrategy.class)
                            .invoke(null, watermarkStrategy);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Class<?> getEventTimeExtensionImplClass() {
        try {
            return Class.forName(
                    "org.apache.flink.datastream.impl.extension.eventtime.EventTimeExtensionImpl");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Please ensure that flink-datastream in your class path");
        }
    }
}
