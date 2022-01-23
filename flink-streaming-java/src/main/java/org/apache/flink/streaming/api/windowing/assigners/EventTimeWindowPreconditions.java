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

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.TimestampsAndWatermarksTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.List;

/** Util class for checking that an event time window is being used in the correct context. */
public class EventTimeWindowPreconditions {

    /**
     * Throws an {@link IllegalUseOfEventTimeWindowException} if the event time window has an
     * invalid preceding watermark generator.
     *
     * <p>Illegal in this context is a watermark generator who does not generate watermarks such as
     * {@link WatermarkStrategy#noWatermarks()}
     *
     * <p>NOTE: If no watermark generator is preceding the event time window no exception is being
     * thrown as it is impossible to check whether the source function is emitting watermarks via
     * {@link
     * org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext#emitWatermark(Watermark)}
     * or not.
     *
     * @param predecessors The list of predecessors of the event time window
     */
    public static void hasInvalidPrecedingWatermarkGenerator(
            final List<Transformation<?>> predecessors) {

        for (int i = predecessors.size() - 1; i >= 0; i--) {
            final Transformation<?> pre = predecessors.get(i);

            if (pre instanceof TimestampsAndWatermarksTransformation) {
                TimestampsAndWatermarksTransformation<?> timestampsAndWatermarksTransformation =
                        (TimestampsAndWatermarksTransformation<?>) pre;

                final WatermarkStrategy<?> waStrat =
                        timestampsAndWatermarksTransformation.getWatermarkStrategy();

                // assert that it generates timestamps or throw exception
                if (!waStrat.isEventTime()) {
                    throw new IllegalUseOfEventTimeWindowException();
                }

                // We have to terminate the check now as we have found the first most recent
                // timestamp assigner for this window and ensured that it actually adds event
                // time stamps. If there has been previously in the chain a window assigner
                // such as noWatermarks() we can safely ignore it as another valid event time
                // watermark assigner
                // exists in the chain after and before our current event time window.
                break;
            }
        }

        // End of line reached: Maybe the user uses a Source function and emits
        // watermarks via emitWatermark(), maybe he doesn't, but we cannot check for that.
        // TODO: Should we log a notice/warning which can be turned off via settings or leave it
        //       up to the user?
    }

    /**
     * Custom exception which is being thrown in case an event time window has an illegal preceding
     * watermark generator.
     */
    public static class IllegalUseOfEventTimeWindowException extends IllegalArgumentException {
        public IllegalUseOfEventTimeWindowException() {
            super(
                    "Cannot use an EventTime window with a preceding water mark generator which"
                            + " does not ingest event times. Did you use noWatermarks() as the WatermarkStrategy"
                            + " and used EventTime windows such as SlidingEventTimeWindows/SlidingEventTimeWindows ?"
                            + " These windows will never window any values as your stream does not support event time");
        }
    }
}
