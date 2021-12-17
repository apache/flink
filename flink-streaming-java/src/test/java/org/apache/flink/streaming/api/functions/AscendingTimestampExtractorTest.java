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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Tests for {@link AscendingTimestampExtractor}. */
public class AscendingTimestampExtractorTest {

    @Test
    public void testWithFailingHandler() {
        AscendingTimestampExtractor<Long> extractor =
                new LongExtractor()
                        .withViolationHandler(new AscendingTimestampExtractor.FailingHandler());

        runValidTests(extractor);
        try {
            runInvalidTest(extractor);
            fail("should fail with an exception");
        } catch (Exception ignored) {
        }
    }

    @Test
    public void testWithIgnoringHandler() {
        AscendingTimestampExtractor<Long> extractor =
                new LongExtractor()
                        .withViolationHandler(new AscendingTimestampExtractor.IgnoringHandler());

        runValidTests(extractor);
        runInvalidTest(extractor);
    }

    @Test
    public void testWithLoggingHandler() {
        AscendingTimestampExtractor<Long> extractor =
                new LongExtractor()
                        .withViolationHandler(new AscendingTimestampExtractor.LoggingHandler());

        runValidTests(extractor);
        runInvalidTest(extractor);
    }

    @Test
    public void testWithDefaultHandler() {
        AscendingTimestampExtractor<Long> extractor = new LongExtractor();

        runValidTests(extractor);
        runInvalidTest(extractor);
    }

    @Test
    public void testInitialAndFinalWatermark() {
        AscendingTimestampExtractor<Long> extractor = new LongExtractor();
        assertEquals(Long.MIN_VALUE, extractor.getCurrentWatermark().getTimestamp());

        extractor.extractTimestamp(Long.MIN_VALUE, -1L);

        extractor.extractTimestamp(Long.MAX_VALUE, -1L);
        assertEquals(Long.MAX_VALUE - 1, extractor.getCurrentWatermark().getTimestamp());
    }

    // ------------------------------------------------------------------------

    private void runValidTests(AscendingTimestampExtractor<Long> extractor) {
        assertEquals(13L, extractor.extractTimestamp(13L, -1L));
        assertEquals(13L, extractor.extractTimestamp(13L, 0L));
        assertEquals(14L, extractor.extractTimestamp(14L, 0L));
        assertEquals(20L, extractor.extractTimestamp(20L, 0L));
        assertEquals(20L, extractor.extractTimestamp(20L, 0L));
        assertEquals(20L, extractor.extractTimestamp(20L, 0L));
        assertEquals(500L, extractor.extractTimestamp(500L, 0L));

        assertEquals(Long.MAX_VALUE - 1, extractor.extractTimestamp(Long.MAX_VALUE - 1, 99999L));
    }

    private void runInvalidTest(AscendingTimestampExtractor<Long> extractor) {
        assertEquals(1000L, extractor.extractTimestamp(1000L, 100));
        assertEquals(1000L, extractor.extractTimestamp(1000L, 100));

        // violation
        assertEquals(999L, extractor.extractTimestamp(999L, 100));
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
