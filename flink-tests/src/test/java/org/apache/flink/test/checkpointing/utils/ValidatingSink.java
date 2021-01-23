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

package org.apache.flink.test.checkpointing.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.test.util.SuccessException;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Generalized sink for validation of window checkpointing IT cases. */
public class ValidatingSink<T> extends RichSinkFunction<T>
        implements ListCheckpointed<HashMap<Long, Integer>> {

    /** Function to check if the window counts are as expected. */
    @FunctionalInterface
    public interface ResultChecker extends Serializable {
        boolean checkResult(Map<Long, Integer> windowCounts);
    }

    /**
     * Function that updates the window counts from an update event.
     *
     * @param <T> type of the update event.
     */
    public interface CountUpdater<T> extends Serializable {
        void updateCount(T update, Map<Long, Integer> windowCounts);
    }

    @Nonnull private final ResultChecker resultChecker;

    @Nonnull private final CountUpdater<T> countUpdater;

    @Nonnull private final HashMap<Long, Integer> windowCounts;

    private final boolean usingProcessingTime;

    public ValidatingSink(
            @Nonnull CountUpdater<T> countUpdater, @Nonnull ResultChecker resultChecker) {
        this(countUpdater, resultChecker, false);
    }

    public ValidatingSink(
            @Nonnull CountUpdater<T> countUpdater,
            @Nonnull ResultChecker resultChecker,
            boolean usingProcessingTime) {

        this.resultChecker = resultChecker;
        this.countUpdater = countUpdater;
        this.usingProcessingTime = usingProcessingTime;
        this.windowCounts = new HashMap<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // this sink can only work with DOP 1
        assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
        if (usingProcessingTime && resultChecker.checkResult(windowCounts)) {
            throw new SuccessException();
        }
    }

    @Override
    public void close() {
        if (resultChecker.checkResult(windowCounts)) {
            if (usingProcessingTime) {
                throw new SuccessException();
            }
        } else {
            throw new AssertionError("Test failed check.");
        }
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        countUpdater.updateCount(value, windowCounts);
        if (usingProcessingTime && resultChecker.checkResult(windowCounts)) {
            throw new SuccessException();
        }
    }

    @Override
    public List<HashMap<Long, Integer>> snapshotState(long checkpointId, long timestamp)
            throws Exception {
        return Collections.singletonList(this.windowCounts);
    }

    @Override
    public void restoreState(List<HashMap<Long, Integer>> state) throws Exception {
        if (state.isEmpty() || state.size() > 1) {
            throw new RuntimeException(
                    "Test failed due to unexpected recovered state size " + state.size());
        }
        windowCounts.putAll(state.get(0));
    }
}
