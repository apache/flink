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

package org.apache.flink.connector.file.src.util;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link ArrayResultIterator}. */
class ArrayResultIteratorTest {

    @Test
    void testEmptyConstruction() {
        final ArrayResultIterator<Object> iter = new ArrayResultIterator<>();
        assertThat(iter.next()).isNull();
    }

    @Test
    void testGetElements() {
        final String[] elements = new String[] {"1", "2", "3", "4"};
        final long initialPos = 1422;
        final long initialSkipCount = 17;

        final ArrayResultIterator<String> iter = new ArrayResultIterator<>();
        iter.set(elements, elements.length, initialPos, initialSkipCount);

        for (int i = 0; i < elements.length; i++) {
            final RecordAndPosition<String> recAndPos = iter.next();
            assertThat(recAndPos.getRecord()).isEqualTo(elements[i]);
            assertThat(recAndPos.getOffset()).isEqualTo(initialPos);
            assertThat(recAndPos.getRecordSkipCount()).isEqualTo(initialSkipCount + i + 1);
        }
    }

    @Test
    void testExhausted() {
        final ArrayResultIterator<String> iter = new ArrayResultIterator<>();
        iter.set(new String[] {"1", "2"}, 2, 0L, 0L);

        iter.next();
        iter.next();

        assertThat(iter.next()).isNull();
    }

    @Test
    void testArraySubRange() {
        final ArrayResultIterator<String> iter = new ArrayResultIterator<>();
        iter.set(new String[] {"1", "2", "3"}, 2, 0L, 0L);

        assertThat(iter.next()).isNotNull();
        assertThat(iter.next()).isNotNull();
        assertThat(iter.next()).isNull();
    }

    @Test
    void testNoRecycler() {
        final ArrayResultIterator<Object> iter = new ArrayResultIterator<>();
        iter.releaseBatch();
    }

    @Test
    void testRecycler() {
        final AtomicBoolean recycled = new AtomicBoolean();
        final ArrayResultIterator<Object> iter =
                new ArrayResultIterator<>(() -> recycled.set(true));

        iter.releaseBatch();

        assertThat(recycled.get()).isTrue();
    }
}
