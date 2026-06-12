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

import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilteringCloseableIteratorTest {

    @Test
    void predicateMatchReturnsMatchingElements() throws Exception {
        try (FilteringCloseableIterator<Integer> iterator =
                new FilteringCloseableIterator<>(
                        CloseableIterator.fromList(Arrays.asList(1, 2, 3, 4), value -> {}),
                        value -> value % 2 == 0)) {
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isEqualTo(2);
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isEqualTo(4);
            assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    void predicateSkipReturnsNoElements() throws Exception {
        try (FilteringCloseableIterator<Integer> iterator =
                new FilteringCloseableIterator<>(
                        CloseableIterator.fromList(Arrays.asList(1, 2, 3), value -> {}),
                        value -> value > 10)) {
            assertThat(iterator.hasNext()).isFalse();
            assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    void emptySourceReturnsNoElements() throws Exception {
        try (FilteringCloseableIterator<Integer> iterator =
                new FilteringCloseableIterator<>(
                        CloseableIterator.fromList(Collections.<Integer>emptyList(), value -> {}),
                        value -> true)) {
            assertThat(iterator.hasNext()).isFalse();
            assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    void closeDelegatesToSource() throws Exception {
        final TestCloseableIterator source = new TestCloseableIterator(Arrays.asList(1, 2, 3));
        final FilteringCloseableIterator<Integer> iterator =
                new FilteringCloseableIterator<>(source, value -> true);

        iterator.close();

        assertThat(source.closed).isTrue();
    }

    @Test
    void hasNextMultipleCallsDoesNotAdvanceTwice() throws Exception {
        try (FilteringCloseableIterator<Integer> iterator =
                new FilteringCloseableIterator<>(
                        CloseableIterator.fromList(Arrays.asList(1, 2, 3), value -> {}),
                        value -> value >= 2)) {
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isEqualTo(2);
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isEqualTo(3);
            assertThat(iterator.hasNext()).isFalse();
        }
    }

    private static final class TestCloseableIterator implements CloseableIterator<Integer> {
        private final Iterator<Integer> iterator;
        private boolean closed;

        private TestCloseableIterator(List<Integer> values) {
            this.iterator = values.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Integer next() {
            return iterator.next();
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
