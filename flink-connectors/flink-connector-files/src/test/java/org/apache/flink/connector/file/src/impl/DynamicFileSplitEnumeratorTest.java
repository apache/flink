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

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.SimpleSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.DynamicFileEnumerator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DynamicFileSplitEnumerator}. */
class DynamicFileSplitEnumeratorTest {

    @Test
    void testEnumerating() {
        String[] splits = new String[] {"0", "1", "2", "3", "4"};
        MockSplitEnumeratorContext<TestSplit> context = new MockSplitEnumeratorContext<>(1);
        context.registerReader(new ReaderInfo(0, ""));

        DynamicFileSplitEnumerator<TestSplit> enumerator =
                new DynamicFileSplitEnumerator<>(
                        context,
                        () -> new TestDynamicFileEnumerator(splits, splits),
                        SimpleSplitAssigner::new);

        for (String ignored : splits) {
            enumerator.handleSplitRequest(0, null);
        }

        assertThat(getAssignedSplits(context)).containsExactlyInAnyOrder(splits);
    }

    @Test
    void testDynamicFiltering() {
        String[] splits = new String[] {"0", "1", "2", "3", "4"};
        String[] remainingSplits = new String[] {"1", "3"};
        MockSplitEnumeratorContext<TestSplit> context = new MockSplitEnumeratorContext<>(1);
        context.registerReader(new ReaderInfo(0, ""));

        DynamicFileSplitEnumerator<TestSplit> enumerator =
                new DynamicFileSplitEnumerator<>(
                        context,
                        () -> new TestDynamicFileEnumerator(splits, remainingSplits),
                        SimpleSplitAssigner::new);
        enumerator.handleSourceEvent(0, mockDynamicFilteringEvent());

        enumerator.handleSplitRequest(0, null);
        enumerator.handleSplitRequest(0, null);

        assertThat(getAssignedSplits(context)).containsExactlyInAnyOrder(remainingSplits);
    }

    @Test
    void testReceiveDynamicFilteringDataAfterStarted() {
        String[] splits = new String[] {"0", "1", "2", "3", "4"};
        String[] remainingSplits = new String[] {"1", "3"};
        MockSplitEnumeratorContext<TestSplit> context = new MockSplitEnumeratorContext<>(1);
        context.registerReader(new ReaderInfo(0, ""));

        DynamicFileSplitEnumerator<TestSplit> enumerator =
                new DynamicFileSplitEnumerator<>(
                        context,
                        () -> new TestDynamicFileEnumerator(splits, remainingSplits),
                        SimpleSplitAssigner::new);

        enumerator.handleSplitRequest(0, null);

        List<String> alreadyAssigned = getAssignedSplits(context);

        enumerator.handleSourceEvent(0, mockDynamicFilteringEvent());

        // request more than 5-1=4 times to check whether a split will be assigned twice
        for (int i = 0; i < 6; i++) {
            enumerator.handleSplitRequest(0, null);
        }

        alreadyAssigned.addAll(Arrays.asList(remainingSplits));
        assertThat(getAssignedSplits(context))
                .containsExactlyInAnyOrder(
                        alreadyAssigned.stream().distinct().toArray(String[]::new));
    }

    @Test
    void testAddSplitsBack() {
        String[] splits = new String[] {"0", "1", "2", "3", "4"};
        String[] remainingSplits = new String[] {"1", "3"};
        MockSplitEnumeratorContext<TestSplit> context = new MockSplitEnumeratorContext<>(1);
        context.registerReader(new ReaderInfo(0, ""));

        DynamicFileSplitEnumerator<TestSplit> enumerator =
                new DynamicFileSplitEnumerator<>(
                        context,
                        () -> new TestDynamicFileEnumerator(splits, remainingSplits),
                        SimpleSplitAssigner::new);

        for (String ignored : splits) {
            enumerator.handleSplitRequest(0, null);
        }

        enumerator.handleSourceEvent(0, mockDynamicFilteringEvent());

        enumerator.addSplitsBack(
                Arrays.stream(splits).map(TestSplit::new).collect(Collectors.toList()), 0);

        for (String ignored : splits) {
            enumerator.handleSplitRequest(0, null);
        }

        List<String> assignedSplits = getAssignedSplits(context);
        assertThat(assignedSplits.subList(5, assignedSplits.size()))
                .containsExactlyInAnyOrder(remainingSplits);
    }

    private static SourceEvent mockDynamicFilteringEvent() {
        // Mock a DynamicFilteringData, typeInfo and rowType of which are not used.
        return new DynamicFilteringEvent(
                new DynamicFilteringData(
                        new GenericTypeInfo<>(RowData.class),
                        RowType.of(),
                        Collections.emptyList(),
                        false));
    }

    private static List<String> getAssignedSplits(MockSplitEnumeratorContext<TestSplit> context) {
        return context.getSplitsAssignmentSequence().stream()
                .flatMap(s -> s.assignment().get(0).stream())
                .map(FileSourceSplit::splitId)
                .collect(Collectors.toList());
    }

    private static class TestSplit extends FileSourceSplit {
        public TestSplit(String id) {
            super(id, new Path(), 0, 0, 0L, 0);
        }
    }

    private static class TestDynamicFileEnumerator implements DynamicFileEnumerator {
        private final List<String> remainingSplits;
        private List<String> enumeratingSplits;

        private TestDynamicFileEnumerator(String[] allSplits, String[] remainingSplits) {
            this.remainingSplits = Arrays.asList(remainingSplits);
            this.enumeratingSplits = Arrays.asList(allSplits);
        }

        @Override
        public void setDynamicFilteringData(DynamicFilteringData data) {
            // Mock filtering result
            enumeratingSplits = remainingSplits;
        }

        @Override
        public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits) {
            return enumeratingSplits.stream().map(TestSplit::new).collect(Collectors.toList());
        }
    }
}
