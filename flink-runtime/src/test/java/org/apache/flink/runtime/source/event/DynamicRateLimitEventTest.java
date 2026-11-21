/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.source.event;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link DynamicRateLimitEvent}. */
class DynamicRateLimitEventTest {

    @Test
    void testGetTargetRecordsPerSecond() {
        DynamicRateLimitEvent event = new DynamicRateLimitEvent(42.5);

        assertThat(event.getRecordsPerSecond()).isEqualTo(42.5);
    }

    @Test
    void testIsLossTolerant() {
        assertThat(new DynamicRateLimitEvent(1.0).isLossTolerant()).isTrue();
    }

    @Test
    void testEqualsAndHashCode() {
        DynamicRateLimitEvent event1 = new DynamicRateLimitEvent(100.0);
        DynamicRateLimitEvent event2 = new DynamicRateLimitEvent(100.0);
        DynamicRateLimitEvent event3 = new DynamicRateLimitEvent(200.0);

        assertThat(event1).isEqualTo(event2).hasSameHashCodeAs(event2);
        assertThat(event1).isNotEqualTo(event3);
        assertThat(event1).isNotEqualTo(null);
        assertThat(event1).isNotEqualTo("not an event");
    }

    @Test
    void testToStringContainsRate() {
        assertThat(new DynamicRateLimitEvent(123.0).toString()).contains("123.0");
    }

    @Test
    void testIsSerializable() throws Exception {
        DynamicRateLimitEvent event = new DynamicRateLimitEvent(77.0);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(baos)) {
            out.writeObject(event);
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            DynamicRateLimitEvent deserialized = (DynamicRateLimitEvent) in.readObject();
            assertThat(deserialized).isEqualTo(event);
        }
    }
}
