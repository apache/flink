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

/** Unit tests for the {@link SingletonResultIterator}. */
class SingletonResultIteratorTest {

    @Test
    void testEmptyConstruction() {
        final SingletonResultIterator<Object> iter = new SingletonResultIterator<>();
        assertThat(iter.next()).isNull();
    }

    @Test
    void testGetElement() {
        final Object element = new Object();
        final long pos = 1422;
        final long skipCount = 17;

        final SingletonResultIterator<Object> iter = new SingletonResultIterator<>();
        iter.set(element, pos, skipCount);

        final RecordAndPosition<Object> record = iter.next();
        assertThat(record).isNotNull();
        assertThat(record.getRecord()).isNotNull();
        assertThat(record.getOffset()).isEqualTo(pos);
        assertThat(record.getRecordSkipCount()).isEqualTo(skipCount);
    }

    @Test
    void testExhausted() {
        final SingletonResultIterator<Object> iter = new SingletonResultIterator<>();
        iter.set(new Object(), 1, 2);
        iter.next();

        assertThat(iter.next()).isNull();
    }

    @Test
    void testNoRecycler() {
        final SingletonResultIterator<Object> iter = new SingletonResultIterator<>();
        iter.releaseBatch();
    }

    @Test
    void testRecycler() {
        final AtomicBoolean recycled = new AtomicBoolean();
        final SingletonResultIterator<Object> iter =
                new SingletonResultIterator<>(() -> recycled.set(true));

        iter.releaseBatch();

        assertThat(recycled.get()).isTrue();
    }
}
