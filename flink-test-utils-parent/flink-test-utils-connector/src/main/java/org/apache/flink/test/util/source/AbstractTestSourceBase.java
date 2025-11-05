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
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * Base class for test sources that allows customization of the enumerator checkpoint type.
 *
 * <p>Most test cases should extend {@link AbstractTestSource} which fixes the enumerator state to
 * {@code Void}. Test cases that need checkpointing can create their own extensions of this base
 * class.
 *
 * @param <T> The type of records produced by this source
 * @param <EnumChkptState> The type of the enumerator checkpoint state
 */
@PublicEvolving
public abstract class AbstractTestSourceBase<T, EnumChkptState>
        implements Source<T, TestSplit, EnumChkptState> {

    private static final long serialVersionUID = 1L;

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    /**
     * Creates a source reader. The default implementation returns an empty reader that immediately
     * returns END_OF_INPUT. Subclasses can override this method to provide their specific reader
     * implementation.
     */
    @Override
    public SourceReader<T, TestSplit> createReader(SourceReaderContext readerContext) {
        return new TestSourceReader<T>(readerContext) {
            @Override
            public InputStatus pollNext(ReaderOutput<T> output) {
                return InputStatus.END_OF_INPUT;
            }
        };
    }

    /**
     * Creates a new split enumerator for fresh starts. Subclasses can override this to provide
     * custom enumerator behavior. The default implementation returns a {@link TestSplitEnumerator}
     * with no checkpoint state.
     *
     * @param enumContext The enumerator context
     * @return The split enumerator
     */
    @Override
    public SplitEnumerator<TestSplit, EnumChkptState> createEnumerator(
            SplitEnumeratorContext<TestSplit> enumContext) {
        return new TestSplitEnumerator<>(enumContext, null);
    }

    /**
     * Restores a split enumerator from a checkpoint. Subclasses can override this to provide custom
     * restoration logic. The default implementation creates a new {@link TestSplitEnumerator} and
     * ignores the checkpoint state.
     *
     * @param enumContext The enumerator context
     * @param checkpoint The checkpoint state to restore from
     * @return The restored split enumerator
     */
    @Override
    public SplitEnumerator<TestSplit, EnumChkptState> restoreEnumerator(
            SplitEnumeratorContext<TestSplit> enumContext, EnumChkptState checkpoint) {
        return new TestSplitEnumerator<>(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<TestSplit> getSplitSerializer() {
        return TestSplit.SERIALIZER;
    }

    /**
     * Returns the serializer for enumerator checkpoints. Subclasses must implement this to provide
     * the appropriate serializer for their checkpoint type.
     */
    @Override
    public abstract SimpleVersionedSerializer<EnumChkptState> getEnumeratorCheckpointSerializer();
}
