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

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Mock {@link TimestampCollector}'s subclass for testing. */
public class TestingTimestampCollector<T> extends TimestampCollector<T> {
    private final Consumer<T> collectConsumer;

    private final BiConsumer<T, Long> collectAndOverwriteTimestampConsumer;

    private TestingTimestampCollector(
            Consumer<T> collectConsumer, BiConsumer<T, Long> collectAndOverwriteTimestampConsuemr) {
        this.collectConsumer = collectConsumer;
        this.collectAndOverwriteTimestampConsumer = collectAndOverwriteTimestampConsuemr;
    }

    @Override
    public void collect(T record) {
        collectConsumer.accept(record);
    }

    @Override
    public void collectAndOverwriteTimestamp(T record, long timestamp) {
        collectAndOverwriteTimestampConsumer.accept(record, timestamp);
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder for {@link TestingTimestampCollector}. */
    public static class Builder<T> {
        private Consumer<T> collectConsumer = (ignore) -> {};

        private BiConsumer<T, Long> collectAndOverwriteTimestampConsumer = (ingore1, ignore2) -> {};

        public Builder<T> setCollectConsumer(Consumer<T> collectConsumer) {
            this.collectConsumer = collectConsumer;
            return this;
        }

        public Builder<T> setCollectAndOverwriteTimestampConsumer(
                BiConsumer<T, Long> collectAndOverwriteTimestampConsumer) {
            this.collectAndOverwriteTimestampConsumer = collectAndOverwriteTimestampConsumer;
            return this;
        }

        public TestingTimestampCollector<T> build() {
            return new TestingTimestampCollector<>(
                    collectConsumer, collectAndOverwriteTimestampConsumer);
        }
    }
}
