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

package org.apache.flink.state.api.input;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test of the buffering collector. */
class BufferingCollectorTest {

    @Test
    void testNestRemovesElement() {
        BufferingCollector<Integer> collector = new BufferingCollector<>();

        collector.collect(1);

        assertThat(collector).as("Failed to add element to collector").hasNext();
        assertThat(collector.next()).as("Incorrect element removed from collector").isOne();
        assertThat(collector).as("Failed to drop element from collector").isExhausted();
    }

    @Test
    void testEmptyCollectorReturnsNull() {
        BufferingCollector<Integer> collector = new BufferingCollector<>();
        assertThat(collector.next()).isNull();
    }
}
