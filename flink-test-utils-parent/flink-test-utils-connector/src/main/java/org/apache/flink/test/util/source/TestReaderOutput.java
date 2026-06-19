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

package org.apache.flink.test.util.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;

import java.util.ArrayList;
import java.util.List;

/**
 * Test utility for capturing output from SourceReader implementations. Provides convenient methods
 * to verify emitted records without implementing the full ReaderOutput interface.
 *
 * @param <T> The type of records emitted by the source
 */
@PublicEvolving
public class TestReaderOutput<T> implements ReaderOutput<T> {

    private final List<T> collected = new ArrayList<>();

    @Override
    public void collect(T record) {
        collected.add(record);
    }

    @Override
    public void collect(T record, long timestamp) {
        collected.add(record);
    }

    @Override
    public void releaseOutputForSplit(String splitId) {
        // No-op for testing
    }

    @Override
    public ReaderOutput<T> createOutputForSplit(String splitId) {
        return this;
    }

    @Override
    public void markIdle() {
        // No-op for testing
    }

    @Override
    public void emitWatermark(Watermark watermark) {
        // No-op for testing
    }

    @Override
    public void markActive() {
        // No-op for testing
    }

    /**
     * Returns all records collected by this output.
     *
     * @return List of all collected records
     */
    public List<T> getCollected() {
        return new ArrayList<>(collected);
    }

    /**
     * Returns the last emitted record, or null if no records have been emitted.
     *
     * @return The most recently emitted record
     */
    public T getLastEmitted() {
        return collected.isEmpty() ? null : collected.get(collected.size() - 1);
    }

    /**
     * Returns the number of records collected.
     *
     * @return Count of collected records
     */
    public int getCollectedCount() {
        return collected.size();
    }

    /** Clears all collected records. */
    public void clear() {
        collected.clear();
    }
}
