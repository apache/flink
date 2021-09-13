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

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Source split that wraps the actual split type. */
public class HybridSourceSplit implements SourceSplit {

    private final byte[] wrappedSplitBytes;
    private final int wrappedSplitSerializerVersion;
    private final int sourceIndex;
    private final String splitId;

    public HybridSourceSplit(
            int sourceIndex, byte[] wrappedSplit, int serializerVersion, String splitId) {
        this.sourceIndex = sourceIndex;
        this.wrappedSplitBytes = wrappedSplit;
        this.wrappedSplitSerializerVersion = serializerVersion;
        this.splitId = splitId;
    }

    public int sourceIndex() {
        return this.sourceIndex;
    }

    public byte[] wrappedSplitBytes() {
        return wrappedSplitBytes;
    }

    public int wrappedSplitSerializerVersion() {
        return wrappedSplitSerializerVersion;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HybridSourceSplit that = (HybridSourceSplit) o;
        return sourceIndex == that.sourceIndex
                && Arrays.equals(wrappedSplitBytes, that.wrappedSplitBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(wrappedSplitBytes, sourceIndex);
    }

    @Override
    public String toString() {
        return "HybridSourceSplit{" + "sourceIndex=" + sourceIndex + ", splitId=" + splitId + '}';
    }

    public static List<HybridSourceSplit> wrapSplits(
            List<? extends SourceSplit> state, int readerIndex, SwitchedSources switchedSources) {
        List<HybridSourceSplit> wrappedSplits = new ArrayList<>(state.size());
        for (SourceSplit split : state) {
            wrappedSplits.add(wrapSplit(split, readerIndex, switchedSources));
        }
        return wrappedSplits;
    }

    public static HybridSourceSplit wrapSplit(
            SourceSplit split, int sourceIndex, SwitchedSources switchedSources) {
        try {
            SimpleVersionedSerializer<SourceSplit> serializer =
                    switchedSources.serializerOf(sourceIndex);
            byte[] serialized = serializer.serialize(split);
            return new HybridSourceSplit(
                    sourceIndex, serialized, serializer.getVersion(), split.splitId());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static List<SourceSplit> unwrapSplits(
            List<HybridSourceSplit> splits, SwitchedSources switchedSources) {
        List<SourceSplit> unwrappedSplits = new ArrayList<>(splits.size());
        for (HybridSourceSplit split : splits) {
            unwrappedSplits.add(unwrapSplit(split, switchedSources));
        }
        return unwrappedSplits;
    }

    public static SourceSplit unwrapSplit(
            HybridSourceSplit split, SwitchedSources switchedSources) {
        try {
            return switchedSources
                    .serializerOf(split.sourceIndex())
                    .deserialize(split.wrappedSplitSerializerVersion(), split.wrappedSplitBytes());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
