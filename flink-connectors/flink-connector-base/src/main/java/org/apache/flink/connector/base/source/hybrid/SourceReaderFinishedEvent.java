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

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;

/**
 * A source event sent from the HybridSourceReader to the enumerator to indicate that the current
 * reader has finished and splits for the next reader can be sent.
 */
public class SourceReaderFinishedEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;
    private final int sourceIndex;
    private transient List<HybridSourceSplit> finishedSplits;

    /**
     * Constructor.
     *
     * @param sourceIndex
     */
    public SourceReaderFinishedEvent(int sourceIndex) {
        this(sourceIndex, Collections.emptyList());
    }

    public SourceReaderFinishedEvent(int sourceIndex, List<HybridSourceSplit> finishedSplits) {
        this.sourceIndex = sourceIndex;
        this.finishedSplits = finishedSplits;
    }

    public List<HybridSourceSplit> getFinishedSplits() {
        return finishedSplits;
    }

    public int sourceIndex() {
        return sourceIndex;
    }

    @Override
    public String toString() {
        return "SourceReaderFinishedEvent{" + "sourceIndex=" + sourceIndex + '}';
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        SimpleVersionedSerialization.writeVersionAndSerializeList(
                new HybridSourceSplitSerializer(),
                finishedSplits,
                new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
        in.defaultReadObject();

        finishedSplits =
                SimpleVersionedSerialization.readVersionAndDeserializeList(
                        new HybridSourceSplitSerializer(), new DataInputViewStreamWrapper(in));
    }
}
