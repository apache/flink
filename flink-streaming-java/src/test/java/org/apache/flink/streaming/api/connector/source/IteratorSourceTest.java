/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.streaming.util.SourceTestHarnessUtils;
import org.apache.flink.util.IterableUtils;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** {@link IteratorSource} test. */
public class IteratorSourceTest {
    private static final int TEST_ELEMENTS_COUNT = 10000;

    @Test
    public void testReadNonSerializableElements() throws Exception {
        CountIterator iterator = new CountIterator(TEST_ELEMENTS_COUNT);
        IteratorSource<Integer> source = new IteratorSource<>(iterator);
        List<Integer> result =
                SourceTestHarnessUtils.testBoundedSourceWithHarness(
                        source,
                        TEST_ELEMENTS_COUNT,
                        Collections.singletonList(new IteratorSplit<>(iterator)));

        List<Integer> expected =
                IterableUtils.toStream(() -> new CountIterator(TEST_ELEMENTS_COUNT))
                        .collect(Collectors.toList());
        assertEquals(expected, result);
    }

    /** Simple serializable iterator, returns numbers in order up to the transmitted limit. */
    public static class CountIterator implements Iterator<Integer>, Serializable {

        private final int limit;

        private int offset = 0;

        public CountIterator(int limit) {
            this.limit = limit;
        }

        @Override
        public boolean hasNext() {
            return offset < limit;
        }

        @Override
        public Integer next() {
            return offset++;
        }
    }
}
