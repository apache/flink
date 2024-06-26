/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.eventtime.TimestampWatermark;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.streaming.api.watermark.WatermarkEvent;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IngestionTimeExtractor}. */
class IngestionTimeExtractorTest {

    @Test
    void testMonotonousTimestamps() {
        AssignerWithPeriodicWatermarks<String> assigner = new IngestionTimeExtractor<>();

        long maxRecordSoFar = 0L;
        long maxWatermarkSoFar = 0L;

        for (int i = 0; i < 1343; i++) {
            if (i % 7 == 1) {
                WatermarkEvent mark = assigner.getCurrentWatermark();
                assertThat(mark).isNotNull();

                Watermark genericWatermark = mark.getWatermark();
                assertThat(genericWatermark).isInstanceOf(TimestampWatermark.class);

                // increasing watermarks
                assertThat(((TimestampWatermark) genericWatermark).getTimestamp())
                        .isGreaterThanOrEqualTo(maxWatermarkSoFar);
                maxWatermarkSoFar = ((TimestampWatermark) genericWatermark).getTimestamp();

                // tight watermarks
                assertThat(((TimestampWatermark) genericWatermark).getTimestamp())
                        .isGreaterThanOrEqualTo(maxRecordSoFar - 1);
            } else {
                long next = assigner.extractTimestamp("a", Long.MIN_VALUE);

                // increasing timestamps
                assertThat(next).isGreaterThanOrEqualTo(maxRecordSoFar);

                // timestamps are never below or at the watermark
                assertThat(next).isGreaterThanOrEqualTo(maxWatermarkSoFar);

                maxRecordSoFar = next;
            }

            if (i % 9 == 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }
}
