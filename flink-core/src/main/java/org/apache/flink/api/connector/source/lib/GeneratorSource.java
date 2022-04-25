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

package org.apache.flink.api.connector.source.lib;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.NumberSequenceIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.InstantiationUtil.serializeObject;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Public
public class GeneratorSource<OUT>
        implements Source<OUT, NumberSequenceSplit<OUT>, Collection<NumberSequenceSplit<OUT>>>,
                ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    /** The end number in the sequence, inclusive. */
    private final long count;

    private final TypeInformation<OUT> typeInfo;

    public final MapFunction<Long, OUT> mapFunction;

    public final Boundedness boundedness;

    public long getCount() {
        return count;
    }

    private GeneratorSource(
            MapFunction<Long, OUT> mapFunction, long count, TypeInformation<OUT> typeInfo) {
        this.mapFunction = checkNotNull(mapFunction);
        checkArgument(count > 0, "count must be > 0");
        this.count = count;
        this.typeInfo = typeInfo;
        boundedness =
                count == Long.MAX_VALUE ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    public static <OUT> GeneratorSource<OUT> from(
            MapFunction<Long, OUT> mapFunction, long count, TypeInformation<OUT> typeInfo) {
        return new GeneratorSource<>(mapFunction, count, typeInfo);
    }

    // ------------------------------------------------------------------------
    //  source methods
    // ------------------------------------------------------------------------

    @Override
    public TypeInformation<OUT> getProducedType() {
        return typeInfo;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<OUT, NumberSequenceSplit<OUT>> createReader(
            SourceReaderContext readerContext) {
        return new IteratorSourceReader<>(readerContext);
    }

    @Override
    public SplitEnumerator<NumberSequenceSplit<OUT>, Collection<NumberSequenceSplit<OUT>>>
            createEnumerator(SplitEnumeratorContext<NumberSequenceSplit<OUT>> enumContext)
                    throws Exception {
        final List<NumberSequenceSplit<OUT>> splits =
                splitNumberRange(count, enumContext.currentParallelism());
        return new IteratorSourceEnumerator<>(enumContext, splits);
    }

    @Override
    public SplitEnumerator<NumberSequenceSplit<OUT>, Collection<NumberSequenceSplit<OUT>>>
            restoreEnumerator(
                    SplitEnumeratorContext<NumberSequenceSplit<OUT>> enumContext,
                    Collection<NumberSequenceSplit<OUT>> checkpoint)
                    throws Exception {
        return new IteratorSourceEnumerator<>(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<NumberSequenceSplit<OUT>> getSplitSerializer() {
        return new SplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<NumberSequenceSplit<OUT>>>
            getEnumeratorCheckpointSerializer() {
        return new CheckpointSerializer();
    }

    protected List<NumberSequenceSplit<OUT>> splitNumberRange(long to, int numSplits) {
        final NumberSequenceIterator[] subSequences =
                new NumberSequenceIterator(0, to).split(numSplits);
        final ArrayList<NumberSequenceSplit<OUT>> splits = new ArrayList<>(subSequences.length);

        int splitId = 1;
        for (NumberSequenceIterator seq : subSequences) {
            if (seq.hasNext()) {
                splits.add(
                        new NumberSequenceSplit<>(
                                String.valueOf(splitId++),
                                seq.getCurrent(),
                                seq.getTo(),
                                mapFunction));
            }
        }

        return splits;
    }

    // ------------------------------------------------------------------------
    //  splits & checkpoint
    // ------------------------------------------------------------------------

    private final class SplitSerializer
            implements SimpleVersionedSerializer<NumberSequenceSplit<OUT>> {

        private static final int CURRENT_VERSION = 1;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(NumberSequenceSplit<OUT> split) throws IOException {
            checkArgument(
                    split.getClass() == NumberSequenceSplit.class, "cannot serialize subclasses");

            // We will serialize 2 longs (16 bytes) plus the UFT representation of the string (2 +
            // length)
            final DataOutputSerializer out =
                    new DataOutputSerializer(split.splitId().length() + 18);
            serializeV1(out, split);
            return out.getCopyOfBuffer();
        }

        @Override
        public NumberSequenceSplit<OUT> deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != CURRENT_VERSION) {
                throw new IOException("Unrecognized version: " + version);
            }
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            return deserializeV1(in);
        }
    }

    void serializeV1(DataOutputView out, NumberSequenceSplit<OUT> split) throws IOException {
        out.writeUTF(split.splitId());
        out.writeLong(split.from());
        out.writeLong(split.to());
        out.write(serializeObject(split.mapFunction()));
    }

    NumberSequenceSplit<OUT> deserializeV1(DataInputView in) throws IOException {
        return new NumberSequenceSplit<>(in.readUTF(), in.readLong(), in.readLong(), mapFunction);
    }

    private final class CheckpointSerializer
            implements SimpleVersionedSerializer<Collection<NumberSequenceSplit<OUT>>> {

        private static final int CURRENT_VERSION = 1;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        // TODO: Adjust!
        public byte[] serialize(Collection<NumberSequenceSplit<OUT>> checkpoint)
                throws IOException {
            // Each split needs 2 longs (16 bytes) plus the UFT representation of the string (2 +
            // length)
            // Assuming at most 4 digit split IDs, 22 bytes per split avoids any intermediate array
            // resizing.
            // plus four bytes for the length field
            final DataOutputSerializer out = new DataOutputSerializer(checkpoint.size() * 22 + 4);
            out.writeInt(checkpoint.size());
            for (NumberSequenceSplit<OUT> split : checkpoint) {
                serializeV1(out, split);
            }
            return out.getCopyOfBuffer();
        }

        @Override
        // TODO: Adjust!
        public Collection<NumberSequenceSplit<OUT>> deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != CURRENT_VERSION) {
                throw new IOException("Unrecognized version: " + version);
            }
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            final int num = in.readInt();
            final ArrayList<NumberSequenceSplit<OUT>> result = new ArrayList<>(num);
            for (int remaining = num; remaining > 0; remaining--) {
                result.add(deserializeV1(in));
            }
            return result;
        }
    }
}
