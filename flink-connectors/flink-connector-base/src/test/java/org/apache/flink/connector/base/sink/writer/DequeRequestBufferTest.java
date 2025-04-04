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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DequeRequestBuffer}. */
public class DequeRequestBufferTest {
    private DequeRequestBuffer<String> bufferWrapper;

    @BeforeEach
    void setUp() {
        bufferWrapper = new DequeRequestBuffer<>();
    }

    /** Test entries should be added in FIFO Fashion. */
    @Test
    void shouldAddEntriesInFifoOrder() {
        RequestEntryWrapper<String> entry1 = new RequestEntryWrapper<>("Entry1", 10);
        RequestEntryWrapper<String> entry2 = new RequestEntryWrapper<>("Entry2", 20);

        bufferWrapper.add(entry1, false);
        bufferWrapper.add(entry2, false);

        assertThat(bufferWrapper.size()).isEqualTo(2);
        assertThat(bufferWrapper.peek()).isEqualTo(entry1);
        assertThat(bufferWrapper.poll()).isEqualTo(entry1);
        assertThat(bufferWrapper.poll()).isEqualTo(entry2);
        assertThat(bufferWrapper.isEmpty()).isTrue();
    }

    /** Test that priority entries are added to the HEAD. */
    @Test
    void shouldPrioritizeEntriesAddedAtHead() {
        RequestEntryWrapper<String> entry1 = new RequestEntryWrapper<>("Entry1", 10);
        RequestEntryWrapper<String> entry2 = new RequestEntryWrapper<>("Entry2", 20);
        RequestEntryWrapper<String> priorityEntry = new RequestEntryWrapper<>("PriorityEntry", 30);

        bufferWrapper.add(entry1, false);
        bufferWrapper.add(entry2, false);
        bufferWrapper.add(priorityEntry, true); // Should be added at the front

        assertThat(bufferWrapper.size()).isEqualTo(3);
        assertThat(bufferWrapper.peek()).isEqualTo(priorityEntry);
        assertThat(bufferWrapper.poll()).isEqualTo(priorityEntry);
        assertThat(bufferWrapper.poll()).isEqualTo(entry1);
        assertThat(bufferWrapper.poll()).isEqualTo(entry2);
    }

    /** Test stack trace correctly. */
    @Test
    void shouldTrackTotalSizeCorrectly() {
        RequestEntryWrapper<String> entry1 = new RequestEntryWrapper<>("Entry1", 10);
        RequestEntryWrapper<String> entry2 = new RequestEntryWrapper<>("Entry2", 20);

        bufferWrapper.add(entry1, false);
        bufferWrapper.add(entry2, false);

        assertThat(bufferWrapper.totalSizeInBytes()).isEqualTo(30);

        bufferWrapper.poll(); // Removes entry1 (10 bytes)
        assertThat(bufferWrapper.totalSizeInBytes()).isEqualTo(20);

        bufferWrapper.poll(); // Removes entry2 (20 bytes)
        assertThat(bufferWrapper.totalSizeInBytes()).isEqualTo(0);
    }

    /** Test get buffered state. */
    @Test
    void shouldReturnBufferedStateSnapshot() {
        RequestEntryWrapper<String> entry1 = new RequestEntryWrapper<>("Entry1", 10);
        RequestEntryWrapper<String> entry2 = new RequestEntryWrapper<>("Entry2", 20);

        bufferWrapper.add(entry1, false);
        bufferWrapper.add(entry2, false);

        List<RequestEntryWrapper<String>> snapshot =
                (List<RequestEntryWrapper<String>>) bufferWrapper.getBufferedState();
        assertThat(snapshot).containsExactly(entry1, entry2);
    }

    /** Test initial buffer. */
    @Test
    void shouldHandleEmptyBufferCorrectly() {
        assertThat(bufferWrapper.isEmpty()).isTrue();
        assertThat(bufferWrapper.poll()).isNull();
        assertThat(bufferWrapper.peek()).isNull();
        assertThat(bufferWrapper.totalSizeInBytes()).isEqualTo(0);
    }
}
