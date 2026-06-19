/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * A reader for {@link ChangelogStateHandleStreamImpl} that iterates over its underlying {@link
 * StreamStateHandle stream handles} and offsets. Starting from each offset, it enumerates the
 * {@link StateChange state changes} using the provided {@link StateChangeIterator}. Different
 * {@link StateChangelogStorage} implementations may have different <b>iterator</b> implementations.
 * Using a different {@link ChangelogStateHandle} (and reader) is problematic as it needs to be
 * serialized.
 */
@Internal
public class StateChangelogHandleStreamHandleReader
        implements StateChangelogHandleReader<ChangelogStateHandleStreamImpl> {
    private static final Logger LOG =
            LoggerFactory.getLogger(StateChangelogHandleStreamHandleReader.class);

    /** Reads a stream of state changes starting from a specified offset. */
    public interface StateChangeIterator {
        CloseableIterator<StateChange> read(StreamStateHandle handle, long offset)
                throws IOException;
    }

    private final StateChangeIterator changeIterator;

    public StateChangelogHandleStreamHandleReader(StateChangeIterator changeIterator) {
        this.changeIterator = changeIterator;
    }

    @Override
    public CloseableIterator<StateChange> getChanges(ChangelogStateHandleStreamImpl handle)
            throws IOException {
        return new CloseableIterator<StateChange>() {
            private final Iterator<Tuple2<StreamStateHandle, Long>> handleIterator =
                    handle.getHandlesAndOffsets().iterator();

            private CloseableIterator<StateChange> current = CloseableIterator.empty();

            @Override
            public boolean hasNext() {
                advance();
                return current.hasNext();
            }

            @Override
            public StateChange next() {
                advance();
                return current.next();
            }

            private void advance() {
                while (!current.hasNext() && handleIterator.hasNext()) {
                    try {
                        current.close();
                        Tuple2<StreamStateHandle, Long> tuple2 = handleIterator.next();
                        LOG.debug("read at {} from {}", tuple2.f1, tuple2.f0);
                        current = changeIterator.read(tuple2.f0, tuple2.f1);
                    } catch (Exception e) {
                        ExceptionUtils.rethrow(e);
                    }
                }
            }

            @Override
            public void close() throws Exception {
                current.close();
            }
        };
    }
}
