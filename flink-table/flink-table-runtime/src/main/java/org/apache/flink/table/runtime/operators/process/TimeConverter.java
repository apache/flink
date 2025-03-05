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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.annotation.Internal;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/** Time converter between internal and external time conversion classes. */
@Internal
abstract class TimeConverter<TimeConversion> {

    abstract TimeConversion toExternal(long timeInternal);

    abstract long toInternal(TimeConversion timeExternal);

    private TimeConverter() {}

    static class InstantTimeConverter extends TimeConverter<Instant> {

        static final TimeConverter<Instant> INSTANCE = new InstantTimeConverter();

        private InstantTimeConverter() {}

        @Override
        Instant toExternal(long timeInternal) {
            return Instant.ofEpochMilli(timeInternal);
        }

        @Override
        long toInternal(Instant timeExternal) {
            return timeExternal.toEpochMilli();
        }
    }

    static class LocalDateTimeConverter extends TimeConverter<LocalDateTime> {

        static final LocalDateTimeConverter INSTANCE = new LocalDateTimeConverter();

        private LocalDateTimeConverter() {}

        @Override
        LocalDateTime toExternal(long timeInternal) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(timeInternal), ZoneOffset.UTC);
        }

        @Override
        long toInternal(LocalDateTime timeExternal) {
            return timeExternal.toInstant(ZoneOffset.UTC).toEpochMilli();
        }
    }

    static class LongTimeConverter extends TimeConverter<Long> {

        static final LongTimeConverter INSTANCE = new LongTimeConverter();

        private LongTimeConverter() {}

        @Override
        Long toExternal(long timeInternal) {
            return timeInternal;
        }

        @Override
        long toInternal(Long timeExternal) {
            return timeExternal;
        }
    }
}
