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

package org.apache.flink.streaming.runtime.streamrecord;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StreamRecord}. */
class StreamRecordTest {

    @Test
    void testWithNoTimestamp() {
        StreamRecord<String> record = new StreamRecord<>("test");

        assertThat(record.isRecord()).isTrue();
        assertThat(record.isWatermark()).isFalse();

        assertThat(record.hasTimestamp()).isFalse();
        assertThat(record.getValue()).isEqualTo("test");

        //		try {
        //			record.getTimestamp();
        //			fail("should throw an exception");
        //		} catch (IllegalStateException e) {
        //			assertTrue(e.getMessage().contains("timestamp"));
        //		}
        // for now, the "no timestamp case" returns Long.MIN_VALUE
        assertThat(record.getTimestamp()).isEqualTo(Long.MIN_VALUE);

        assertThat(record.toString()).isNotNull();
        assertThat(record)
                .hasSameHashCodeAs(new StreamRecord<>("test"))
                .isEqualTo(new StreamRecord<>("test"))
                .isEqualTo(record.asRecord());

        assertThatThrownBy(record::asWatermark)
                .isInstanceOf(ClassCastException.class)
                .hasMessageContaining(
                        "cannot be cast to org.apache.flink.streaming.api.watermark.Watermark");
    }

    @Test
    void testWithTimestamp() {
        StreamRecord<String> record = new StreamRecord<>("foo", 42);

        assertThat(record.isRecord()).isTrue();
        assertThat(record.isWatermark()).isFalse();

        assertThat(record.hasTimestamp()).isTrue();
        assertThat(record.getTimestamp()).isEqualTo(42L);

        assertThat(record.getValue()).isEqualTo("foo");

        assertThat(record.toString()).isNotNull();

        assertThat(record)
                .hasSameHashCodeAs(new StreamRecord<>("foo", 42))
                .doesNotHaveSameHashCodeAs(new StreamRecord<>("foo"));

        assertThat(record)
                .isEqualTo(new StreamRecord<>("foo", 42))
                .isNotEqualTo(new StreamRecord<>("foo"))
                .isEqualTo(record.asRecord());

        assertThatThrownBy(record::asWatermark)
                .isInstanceOf(ClassCastException.class)
                .hasMessageContaining(
                        "cannot be cast to org.apache.flink.streaming.api.watermark.Watermark");
    }

    @Test
    void testAllowedTimestampRange() {
        assertThat(new StreamRecord<>("test", 0).getTimestamp()).isZero();
        assertThat(new StreamRecord<>("test", -1).getTimestamp()).isEqualTo(-1L);
        assertThat(new StreamRecord<>("test", 1).getTimestamp()).isOne();
        assertThat(new StreamRecord<>("test", Long.MIN_VALUE).getTimestamp())
                .isEqualTo(Long.MIN_VALUE);
        assertThat(new StreamRecord<>("test", Long.MAX_VALUE).getTimestamp())
                .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void testReplacePreservesTimestamp() {
        StreamRecord<String> recNoTimestamp = new StreamRecord<>("o sole mio");
        StreamRecord<Integer> newRecNoTimestamp = recNoTimestamp.replace(17);
        assertThat(newRecNoTimestamp.hasTimestamp()).isFalse();

        StreamRecord<String> recWithTimestamp = new StreamRecord<>("la dolce vita", 99);
        StreamRecord<Integer> newRecWithTimestamp = recWithTimestamp.replace(17);

        assertThat(newRecWithTimestamp.hasTimestamp()).isTrue();
        assertThat(newRecWithTimestamp.getTimestamp()).isEqualTo(99L);
    }

    @Test
    void testReplaceWithTimestampOverridesTimestamp() {
        StreamRecord<String> record = new StreamRecord<>("la divina comedia");
        assertThat(record.hasTimestamp()).isFalse();

        StreamRecord<Double> newRecord = record.replace(3.14, 123);
        assertThat(newRecord.hasTimestamp()).isTrue();
        assertThat(newRecord.getTimestamp()).isEqualTo(123L);
    }

    @Test
    void testCopy() {
        StreamRecord<String> recNoTimestamp = new StreamRecord<>("test");
        StreamRecord<String> recNoTimestampCopy = recNoTimestamp.copy("test");
        assertThat(recNoTimestampCopy).isEqualTo(recNoTimestamp);

        StreamRecord<String> recWithTimestamp = new StreamRecord<>("test", 99);
        StreamRecord<String> recWithTimestampCopy = recWithTimestamp.copy("test");
        assertThat(recWithTimestampCopy).isEqualTo(recWithTimestamp);
    }

    @Test
    void testCopyTo() {
        StreamRecord<String> recNoTimestamp = new StreamRecord<>("test");
        StreamRecord<String> recNoTimestampCopy = new StreamRecord<>(null);
        recNoTimestamp.copyTo("test", recNoTimestampCopy);
        assertThat(recNoTimestampCopy).isEqualTo(recNoTimestamp);

        StreamRecord<String> recWithTimestamp = new StreamRecord<>("test", 99);
        StreamRecord<String> recWithTimestampCopy = new StreamRecord<>(null);
        recWithTimestamp.copyTo("test", recWithTimestampCopy);
        assertThat(recWithTimestampCopy).isEqualTo(recWithTimestamp);
    }

    @Test
    void testSetAndEraseTimestamps() {
        StreamRecord<String> rec = new StreamRecord<>("hello");
        assertThat(rec.hasTimestamp()).isFalse();

        rec.setTimestamp(13456L);
        assertThat(rec.hasTimestamp()).isTrue();
        assertThat(rec.getTimestamp()).isEqualTo(13456L);

        rec.eraseTimestamp();
        assertThat(rec.hasTimestamp()).isFalse();
    }
}
