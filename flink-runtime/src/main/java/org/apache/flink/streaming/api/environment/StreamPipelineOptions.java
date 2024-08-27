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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;
import org.apache.flink.streaming.api.TimeCharacteristic;

/**
 * The {@link ConfigOption configuration options} for job execution. Those are stream specific
 * options. See also {@link org.apache.flink.configuration.PipelineOptions}.
 *
 * @deprecated This option class is deprecated in 1.20 and will be removed in 2.0.
 */
@Deprecated
@PublicEvolving
public class StreamPipelineOptions {

    /**
     * @deprecated In Flink 1.12 the default stream time characteristic has been changed to {@link
     *     TimeCharacteristic#EventTime}, thus you don't need to set this option for enabling
     *     event-time support anymore. Explicitly using processing-time windows and timers works in
     *     event-time mode. If you need to disable watermarks, please set {@link
     *     PipelineOptions#AUTO_WATERMARK_INTERVAL} to 0. If you are using {@link
     *     TimeCharacteristic#IngestionTime}, please manually set an appropriate {@link
     *     WatermarkStrategy}. If you are using generic "time window" operations (for example {@link
     *     org.apache.flink.streaming.api.datastream.KeyedStream#timeWindow(org.apache.flink.streaming.api.windowing.time.Time)}
     *     that change behaviour based on the time characteristic, please use equivalent operations
     *     that explicitly specify processing time or event time.
     */
    @Deprecated
    public static final ConfigOption<TimeCharacteristic> TIME_CHARACTERISTIC =
            ConfigOptions.key("pipeline.time-characteristic")
                    .enumType(TimeCharacteristic.class)
                    .defaultValue(TimeCharacteristic.EventTime)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The time characteristic for all created streams, e.g., processing"
                                                    + "time, event time, or ingestion time.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "If you set the characteristic to IngestionTime or EventTime this will set a default "
                                                    + "watermark update interval of 200 ms. If this is not applicable for your application "
                                                    + "you should change it using %s.",
                                            TextElement.code(
                                                    PipelineOptions.AUTO_WATERMARK_INTERVAL.key()))
                                    .build());
}
