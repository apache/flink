/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.util.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Base class for test source readers that provides default implementations for all SourceReader
 * methods. Tests can extend this class and override only the methods they need to customize,
 * typically just {@link #pollNext(ReaderOutput)}.
 *
 * <p>By default, this reader:
 *
 * <ul>
 *   <li>Returns {@link InputStatus#END_OF_INPUT} from {@link #pollNext(ReaderOutput)}
 *   <li>Returns empty list from {@link #snapshotState(long)}
 *   <li>Provides a completed future from {@link #isAvailable()}
 *   <li>No-ops for all other methods
 * </ul>
 *
 * @param <T> The type of records produced by this reader
 */
@PublicEvolving
public class TestSourceReader<T> implements SourceReader<T, TestSplit> {

    protected final SourceReaderContext context;

    public TestSourceReader(SourceReaderContext context) {
        this.context = context;
    }

    @Override
    public void start() {
        // No-op by default
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        // By default, immediately signal end of input
        // Tests should override this method to produce actual records
        return InputStatus.END_OF_INPUT;
    }

    @Override
    public List<TestSplit> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<TestSplit> splits) {
        // No-op by default - most test sources work with a single split
    }

    @Override
    public void notifyNoMoreSplits() {
        // No-op by default
    }

    @Override
    public void close() throws Exception {
        // No-op by default
    }
}
