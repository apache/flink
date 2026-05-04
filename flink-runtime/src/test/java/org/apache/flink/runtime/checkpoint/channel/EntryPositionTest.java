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

package org.apache.flink.runtime.checkpoint.channel;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link EntryPosition}. */
class EntryPositionTest {

    @Test
    void testCompareToOrdersByFileIndexFirst() {
        // Two positions in the same file: offset breaks the tie.
        EntryPosition a = new EntryPosition(0, 0L);
        EntryPosition b = new EntryPosition(0, 100L);
        assertThat(a.compareTo(b)).isNegative();
        assertThat(b.compareTo(a)).isPositive();

        // Different files: file index dominates regardless of offset.
        EntryPosition c = new EntryPosition(1, 0L);
        assertThat(b.compareTo(c)).isNegative();
        EntryPosition d = new EntryPosition(0, Long.MAX_VALUE - 1);
        EntryPosition e = new EntryPosition(1, 0L);
        assertThat(d.compareTo(e)).isNegative();
    }

    @Test
    void testEndIsGreaterThanEveryRealPosition() {
        EntryPosition firstEntry = new EntryPosition(0, 0L);
        EntryPosition lateEntry = new EntryPosition(Integer.MAX_VALUE - 1, Long.MAX_VALUE - 1);
        assertThat(firstEntry.compareTo(EntryPosition.END)).isNegative();
        assertThat(lateEntry.compareTo(EntryPosition.END)).isNegative();
        assertThat(EntryPosition.END.compareTo(EntryPosition.END)).isZero();
    }

    @Test
    void testEqualsAndHashCode() {
        EntryPosition a = new EntryPosition(2, 100L);
        EntryPosition b = new EntryPosition(2, 100L);
        EntryPosition c = new EntryPosition(2, 101L);
        EntryPosition d = new EntryPosition(3, 100L);

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(c);
        assertThat(a).isNotEqualTo(d);
    }
}
