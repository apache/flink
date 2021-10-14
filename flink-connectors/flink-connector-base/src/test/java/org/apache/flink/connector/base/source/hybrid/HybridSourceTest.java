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

package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitEnumerator;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/** Tests for {@link HybridSource}. */
public class HybridSourceTest {

    @Test
    public void testBoundedness() {
        HybridSource<Integer> source;

        source =
                HybridSource.builder(new MockBaseSource(1, 1, Boundedness.BOUNDED))
                        .addSource(new MockBaseSource(1, 1, Boundedness.BOUNDED))
                        .build();
        assertEquals(Boundedness.BOUNDED, source.getBoundedness());

        source =
                HybridSource.builder(new MockBaseSource(1, 1, Boundedness.BOUNDED))
                        .addSource(new MockBaseSource(1, 1, Boundedness.CONTINUOUS_UNBOUNDED))
                        .build();
        assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, source.getBoundedness());

        try {
            HybridSource.builder(new MockBaseSource(1, 1, Boundedness.CONTINUOUS_UNBOUNDED))
                    .addSource(new MockBaseSource(1, 1, Boundedness.CONTINUOUS_UNBOUNDED))
                    .build();
            fail("expected exception");
        } catch (IllegalArgumentException e) {
            // boundedness check to fail
        }
    }

    @Test
    public void testBuilderWithSourceFactory() {
        HybridSource.SourceFactory<Integer, Source<Integer, ?, ?>, MockSplitEnumerator>
                sourceFactory =
                        new HybridSource.SourceFactory<
                                Integer, Source<Integer, ?, ?>, MockSplitEnumerator>() {
                            @Override
                            public Source<Integer, ?, ?> create(
                                    HybridSource.SourceSwitchContext<MockSplitEnumerator> context) {
                                MockSplitEnumerator enumerator = context.getPreviousEnumerator();
                                return new MockBaseSource(1, 1, Boundedness.BOUNDED);
                            }
                        };

        HybridSource<Integer> source =
                new HybridSource.HybridSourceBuilder<Integer, MockSplitEnumerator>()
                        .<MockSplitEnumerator, Source<Integer, ?, ?>>addSource(
                                new MockBaseSource(1, 1, Boundedness.BOUNDED))
                        .addSource(sourceFactory, Boundedness.BOUNDED)
                        .build();
        assertNotNull(source);
    }

    private static class ExtendedMockSplitEnumerator extends MockSplitEnumerator {
        public ExtendedMockSplitEnumerator(
                List<MockSourceSplit> splits, SplitEnumeratorContext<MockSourceSplit> context) {
            super(splits, context);
        }
    }

    @Test
    public void testBuilderWithEnumeratorSuperclass() {
        HybridSource.SourceFactory<Integer, Source<Integer, ?, ?>, MockSplitEnumerator>
                sourceFactory =
                        (HybridSource.SourceFactory<
                                        Integer, Source<Integer, ?, ?>, MockSplitEnumerator>)
                                context -> {
                                    MockSplitEnumerator enumerator =
                                            context.getPreviousEnumerator();
                                    return new MockBaseSource(1, 1, Boundedness.BOUNDED);
                                };

        HybridSource<Integer> source =
                new HybridSource.HybridSourceBuilder<Integer, MockSplitEnumerator>()
                        .<ExtendedMockSplitEnumerator, Source<Integer, ?, ?>>addSource(
                                new MockBaseSource(1, 1, Boundedness.BOUNDED))
                        .addSource(sourceFactory, Boundedness.BOUNDED)
                        .build();
        assertNotNull(source);
    }
}
