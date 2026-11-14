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

/** Unit tests for {@link ReportedRecordsEvent}. */
class ReportedRecordsEventTest {

    @Test
    void testGetters() {
        ReportedRecordsEvent event = new ReportedRecordsEvent(7L, 125L, 2500L);

        assertThat(event.getCheckpointId()).isEqualTo(7L);
        assertThat(event.getNumRecords()).isEqualTo(125L);
        assertThat(event.getElapsedMillis()).isEqualTo(2500L);
    }

    @Test
    void testIsLossTolerant() {
        assertThat(new ReportedRecordsEvent(1L, 1L, 1L).isLossTolerant()).isTrue();
    }

    @Test
    void testEqualsAndHashCode() {
        ReportedRecordsEvent event1 = new ReportedRecordsEvent(1L, 50L, 10L);
        ReportedRecordsEvent event2 = new ReportedRecordsEvent(1L, 50L, 10L);
        ReportedRecordsEvent event3 = new ReportedRecordsEvent(1L, 75L, 10L);

        assertThat(event1).isEqualTo(event2).hasSameHashCodeAs(event2);
        assertThat(event1).isNotEqualTo(event3);
        assertThat(event1).isNotEqualTo(new ReportedRecordsEvent(2L, 50L, 10L));
        assertThat(event1).isNotEqualTo(new ReportedRecordsEvent(1L, 50L, 20L));
        assertThat(event1).isNotEqualTo(null);
        assertThat(event1).isNotEqualTo("not an event");
    }

    @Test
    void testIsSerializable() throws Exception {
        ReportedRecordsEvent event = new ReportedRecordsEvent(3L, 33L, 44L);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(baos)) {
            out.writeObject(event);
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            ReportedRecordsEvent deserialized = (ReportedRecordsEvent) in.readObject();
            assertThat(deserialized).isEqualTo(event);
        }
    }
}
