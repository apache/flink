/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AscendingTimestampExtractor}. */
class AscendingTimestampExtractorTest {

    @Test
    void testWithFailingHandler() {
        AscendingTimestampExtractor<Long> extractor =
                new LongExtractor()
                        .withViolationHandler(new AscendingTimestampExtractor.FailingHandler());

        runValidTests(extractor);
        assertThatThrownBy(() -> runInvalidTest(extractor))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Ascending timestamps condition violated.");
    }

    @Test
    void testWithIgnoringHandler() {
        AscendingTimestampExtractor<Long> extractor =
                new LongExtractor()
                        .withViolationHandler(new AscendingTimestampExtractor.IgnoringHandler());

        runValidTests(extractor);
        runInvalidTest(extractor);
    }

    @Test
    void testWithLoggingHandler() {
        AscendingTimestampExtractor<Long> extractor =
                new LongExtractor()
                        .withViolationHandler(new AscendingTimestampExtractor.LoggingHandler());

        runValidTests(extractor);
        runInvalidTest(extractor);
    }

    @Test
    void testWithDefaultHandler() {
        AscendingTimestampExtractor<Long> extractor = new LongExtractor();

        runValidTests(extractor);
        runInvalidTest(extractor);
    }

    @Test
    void testInitialAndFinalWatermark() {
        AscendingTimestampExtractor<Long> extractor = new LongExtractor();
        assertThat(extractor.getCurrentWatermark().getTimestamp()).isEqualTo(Long.MIN_VALUE);

        extractor.extractTimestamp(Long.MIN_VALUE, -1L);

        extractor.extractTimestamp(Long.MAX_VALUE, -1L);
        assertThat(extractor.getCurrentWatermark().getTimestamp()).isEqualTo(Long.MAX_VALUE - 1);
    }

    // ------------------------------------------------------------------------

    private void runValidTests(AscendingTimestampExtractor<Long> extractor) {
        assertThat(extractor.extractTimestamp(13L, -1L)).isEqualTo(13L);
        assertThat(extractor.extractTimestamp(13L, 0L)).isEqualTo(13L);
        assertThat(extractor.extractTimestamp(14L, 0L)).isEqualTo(14L);
        assertThat(extractor.extractTimestamp(20L, 0L)).isEqualTo(20L);
        assertThat(extractor.extractTimestamp(20L, 0L)).isEqualTo(20L);
        assertThat(extractor.extractTimestamp(20L, 0L)).isEqualTo(20L);
        assertThat(extractor.extractTimestamp(500L, 0L)).isEqualTo(500L);

        assertThat(extractor.extractTimestamp(Long.MAX_VALUE - 1, 99999L))
                .isEqualTo(Long.MAX_VALUE - 1);
    }

    private void runInvalidTest(AscendingTimestampExtractor<Long> extractor) {
        assertThat(extractor.extractTimestamp(1000L, 100)).isEqualTo(1000L);
        assertThat(extractor.extractTimestamp(1000L, 100)).isEqualTo(1000L);

        // violation
        assertThat(extractor.extractTimestamp(999L, 100)).isEqualTo(999L);
    }

    // ------------------------------------------------------------------------

    private static class LongExtractor extends AscendingTimestampExtractor<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(Long element) {
            return element;
        }
    }
}
