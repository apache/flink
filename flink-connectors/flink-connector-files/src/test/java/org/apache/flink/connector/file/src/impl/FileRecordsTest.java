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

import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.connector.file.src.util.SingletonResultIterator;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for the {@link FileRecords} class. */
class FileRecordsTest {

    @Test
    void testEmptySplits() {
        final String split = "empty";
        final FileRecords<Object> records = FileRecords.finishedSplit(split);

        assertThat(records.finishedSplits()).isEqualTo(Collections.singleton(split));
    }

    @Test
    void testMoveToFirstSplit() {
        final String splitId = "splitId";
        final FileRecords<Object> records =
                FileRecords.forRecords(splitId, new SingletonResultIterator<>());

        final String firstSplitId = records.nextSplit();

        assertThat(splitId).isEqualTo(firstSplitId);
    }

    @Test
    void testMoveToSecondSplit() {
        final FileRecords<Object> records =
                FileRecords.forRecords("splitId", new SingletonResultIterator<>());
        records.nextSplit();

        final String secondSplitId = records.nextSplit();

        assertThat(secondSplitId).isNull();
    }

    @Test
    void testRecordsFromFirstSplit() {
        final SingletonResultIterator<String> iter = new SingletonResultIterator<>();
        iter.set("test", 18, 99);
        final FileRecords<String> records = FileRecords.forRecords("splitId", iter);
        records.nextSplit();

        final RecordAndPosition<String> recAndPos = records.nextRecordFromSplit();

        assertThat(recAndPos.getRecord()).isEqualTo("test");
        assertThat(recAndPos.getOffset()).isEqualTo(18);
        assertThat(recAndPos.getRecordSkipCount()).isEqualTo(99);
    }

    @Test
    void testRecordsInitiallyIllegal() {
        final FileRecords<Object> records =
                FileRecords.forRecords("splitId", new SingletonResultIterator<>());

        assertThatThrownBy(records::nextRecordFromSplit).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testRecordsOnSecondSplitIllegal() {
        final FileRecords<Object> records =
                FileRecords.forRecords("splitId", new SingletonResultIterator<>());
        records.nextSplit();
        records.nextSplit();

        assertThatThrownBy(records::nextRecordFromSplit).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testRecycleExhaustedBatch() {
        final AtomicBoolean recycled = new AtomicBoolean(false);
        final SingletonResultIterator<Object> iter =
                new SingletonResultIterator<>(() -> recycled.set(true));
        iter.set(new Object(), 1L, 2L);

        final FileRecords<Object> records = FileRecords.forRecords("test split", iter);
        records.nextSplit();
        records.nextRecordFromSplit();

        // make sure we exhausted the iterator
        assertThat(records.nextRecordFromSplit()).isNull();
        assertThat(records.nextSplit()).isNull();

        records.recycle();
        assertThat(recycled.get()).isTrue();
    }

    @Test
    void testRecycleNonExhaustedBatch() {
        final AtomicBoolean recycled = new AtomicBoolean(false);
        final SingletonResultIterator<Object> iter =
                new SingletonResultIterator<>(() -> recycled.set(true));
        iter.set(new Object(), 1L, 2L);

        final FileRecords<Object> records = FileRecords.forRecords("test split", iter);
        records.nextSplit();

        records.recycle();
        assertThat(recycled.get()).isTrue();
    }
}
