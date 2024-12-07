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

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

/** Test class for rate limiting functionalities of {@link AsyncSinkWriter}. */
class AsyncSinkWriterThrottlingTest {

    @Test
    void testSinkThroughputShouldThrottleToHalfBatchSize() throws Exception {
        int maxBatchSize = 32;
        int maxInFlightRequest = 10;
        int numberOfBatchesToSend = 1000;

        TestSinkInitContext context = new TestSinkInitContextAnyThreadMailbox();

        ThrottlingWriter writer =
                new ThrottlingWriter(
                        (elem, ctx) -> Long.valueOf(elem),
                        context,
                        maxBatchSize,
                        maxInFlightRequest);

        // numberOfBatchesToSend should be high enough to overcome initial transient state
        for (int i = 0; i < numberOfBatchesToSend * maxBatchSize; i++) {
            writer.write(String.valueOf(i));
        }

        /**
         * Throttling limit should be maxBatchSize/2 , worst case margin on throttling (maxBatchSize
         * / 2 + 1)->(maxBatchSize/4) or when scaling up (maxBatchSize/2) -> (maxBatchSize/2 + 10).
         */
        Assertions.assertThat(writer.getInflightMessagesLimit())
                .isGreaterThanOrEqualTo(maxBatchSize / 4);
        Assertions.assertThat(writer.getInflightMessagesLimit())
                .isLessThanOrEqualTo(maxBatchSize / 2 + 10);
    }

    private static class ThrottlingWriter extends AsyncSinkWriter<String, Long> {

        private final ProcessingTimeService timeService;
        private final int maxBatchSize;
        private final Queue<Tuple2<Long, Integer>> requestsData;
        private long sizeOfLast100ms;
        private int inflightMessagesLimit;

        public ThrottlingWriter(
                ElementConverter<String, Long> elementConverter,
                WriterInitContext context,
                int maxBatchSize,
                int maxInFlightRequests) {
            super(
                    elementConverter,
                    context,
                    AsyncSinkWriterConfiguration.builder()
                            .setMaxBatchSize(maxBatchSize)
                            .setMaxBatchSizeInBytes(10_000)
                            .setMaxInFlightRequests(maxInFlightRequests)
                            .setMaxBufferedRequests(10_000)
                            .setMaxTimeInBufferMS(100)
                            .setMaxRecordSizeInBytes(1000)
                            .build(),
                    Collections.emptyList());
            this.maxBatchSize = maxBatchSize;
            this.timeService = context.getProcessingTimeService();
            this.requestsData = new ArrayDeque<>();
            this.inflightMessagesLimit = maxBatchSize;
            this.sizeOfLast100ms = 0;
        }

        public void write(String element) throws IOException, InterruptedException {
            super.write(element, null);
        }

        public int getInflightMessagesLimit() {
            return inflightMessagesLimit;
        }

        @Override
        protected void submitRequestEntries(
                List<Long> requestEntries, ResultHandler<Long> resultHandler) {
            long currentProcessingTime = timeService.getCurrentProcessingTime();
            inflightMessagesLimit = requestEntries.size();

            addRequestDataToQueue(requestEntries.size(), currentProcessingTime);

            if (sizeOfLast100ms > maxBatchSize && requestEntries.size() > 1) {
                resultHandler.retryForEntries(requestEntries);
            } else {
                resultHandler.complete();
            }
        }

        @Override
        protected long getSizeInBytes(Long requestEntry) {
            return 8;
        }

        private void addRequestDataToQueue(int size, long time) {
            requestsData.add(Tuple2.of(time, size));

            sizeOfLast100ms += size;
            while (!requestsData.isEmpty() && requestsData.peek().f0 < time - 100L) {
                sizeOfLast100ms -= requestsData.remove().f1;
            }
        }
    }
}
