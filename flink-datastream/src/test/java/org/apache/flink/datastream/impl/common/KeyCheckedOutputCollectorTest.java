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

package org.apache.flink.datastream.impl.common;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KeyCheckedOutputCollector}. */
class KeyCheckedOutputCollectorTest {
    @Test
    void testCollect() {
        TestingTimestampCollector.Builder<Integer> builder = TestingTimestampCollector.builder();
        CompletableFuture<Integer> consumeRecord = new CompletableFuture<>();
        builder.setCollectConsumer(consumeRecord::complete);
        KeyCheckedOutputCollector<Integer, Integer> collector =
                new KeyCheckedOutputCollector<>(builder.build(), (ignore) -> 1, () -> 1);

        final int record = 1;
        collector.collect(record);
        assertThat(consumeRecord).isCompletedWithValue(record);
    }

    @Test
    void testCollectAndOverwriteTimestamp() {
        TestingTimestampCollector.Builder<Integer> builder = TestingTimestampCollector.builder();
        CompletableFuture<Integer> consumeRecord = new CompletableFuture<>();
        CompletableFuture<Long> consumeTimeStamp = new CompletableFuture<>();
        builder.setCollectAndOverwriteTimestampConsumer(
                (data, timeStamp) -> {
                    consumeRecord.complete(data);
                    consumeTimeStamp.complete(timeStamp);
                });

        final int record = 1;
        final long timeStamp = 10L;
        KeyCheckedOutputCollector<Integer, Integer> collector =
                new KeyCheckedOutputCollector<>(builder.build(), (ignore) -> 1, () -> 1);
        collector.collectAndOverwriteTimestamp(record, timeStamp);
        assertThat(consumeRecord).isCompletedWithValue(record);
        assertThat(consumeTimeStamp).isCompletedWithValue(timeStamp);
    }

    @Test
    void testNotEqualToCurrentKey() {
        TestingTimestampCollector.Builder<Integer> builder = TestingTimestampCollector.builder();
        KeyCheckedOutputCollector<Integer, Integer> collector =
                new KeyCheckedOutputCollector<>(builder.build(), (ignore) -> 1, () -> 2);
        assertThatThrownBy(() -> collector.collect(1)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> collector.collectAndOverwriteTimestamp(1, 10L))
                .isInstanceOf(IllegalStateException.class);
    }
}
