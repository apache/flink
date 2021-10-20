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

package org.apache.flink.table.planner.factories.source;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.operators.source.SourceOutputWithWatermarks;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SplittableIterator;

import org.apache.commons.collections.IteratorUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/** A data source implementation for {@link TestValuesTableFactory}. */
public class TestValuesSource
        implements Source<
                RowData,
                TestValuesSource.TestValuesSplit,
                Collection<TestValuesSource.TestValuesSplit>> {

    private transient Iterable<RowData> elements;

    private final int elementNums;

    private byte[] elementsSerialized;

    private final TypeSerializer<RowData> serializer;

    public TestValuesSource(
            TypeSerializer<RowData> serializer, Iterable<RowData> elements, int elementNums)
            throws IOException {
        this.serializer = serializer;
        this.elements = elements;
        this.elementNums = elementNums;
        serializeElements();
    }

    private void serializeElements() throws IOException {
        Preconditions.checkState(serializer != null, "serializer is not set");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
        try {
            for (RowData element : elements) {
                serializer.serialize(element, wrapper);
            }
        } catch (IOException e) {
            throw new IOException("Serializing the source elements failed: " + e.getMessage(), e);
        }
        this.elementsSerialized = baos.toByteArray();
    }

    private void deserializeElements() throws IOException {
        Preconditions.checkState(serializer != null, "serializer is not set");
        Preconditions.checkState(
                elementsSerialized != null && elementsSerialized.length != 0,
                "elementsSerialized doesn't exist");
        ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
        final DataInputView input = new DataInputViewStreamWrapper(bais);

        List<RowData> elements = new ArrayList<>();

        int index = 0;
        while (index < elementNums) {
            try {
                RowData element = serializer.deserialize(input);
                elements.add(element);
                index++;
            } catch (IOException e) {
                throw new IOException(
                        "Deserializing the source elements failed: " + e.getMessage(), e);
            }
        }
        this.elements = elements;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<RowData, TestValuesSplit> createReader(SourceReaderContext readerContext) {
        return new TestValuesReader(readerContext);
    }

    @Override
    public SplitEnumerator<TestValuesSplit, Collection<TestValuesSplit>> createEnumerator(
            SplitEnumeratorContext<TestValuesSplit> enumContext) throws IOException {
        final int currentParallelism = enumContext.currentParallelism();

        if (elements == null) {
            deserializeElements();
        }

        final TestValuesIterator[] subIterators =
                new TestValuesIterator(elements.iterator()).split(currentParallelism);

        final List<TestValuesSplit> splits = new ArrayList<>(subIterators.length);
        int splitId = 1;
        for (TestValuesIterator it : subIterators) {
            if (it.hasNext()) {
                splits.add(new TestValuesSplit(String.valueOf(splitId++), it.getElements()));
            }
        }

        return new IteratorSourceEnumerator<>(enumContext, splits);
    }

    @Override
    public SplitEnumerator<TestValuesSplit, Collection<TestValuesSplit>> restoreEnumerator(
            SplitEnumeratorContext<TestValuesSplit> enumContext,
            Collection<TestValuesSplit> checkpoint) {
        return new IteratorSourceEnumerator<>(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<TestValuesSplit> getSplitSerializer() {
        Preconditions.checkState(serializer != null, "serializer is not set");
        return new TestValuesSplitSerializer(serializer);
    }

    @Override
    public SimpleVersionedSerializer<Collection<TestValuesSplit>>
            getEnumeratorCheckpointSerializer() {
        Preconditions.checkState(serializer != null, "serializer is not set");
        return new TestValuesSourceEnumeratorSerializer(serializer);
    }

    /** A source split for {@link TestValuesSource}. */
    public static class TestValuesSplit
            implements IteratorSourceSplit<RowData, TestValuesIterator> {

        private final String splitId;
        private final Iterator<RowData> elements;

        public TestValuesSplit(String splitId, Iterator<RowData> elements) {
            this.splitId = splitId;
            this.elements = elements;
        }

        @Override
        public TestValuesIterator getIterator() {
            return new TestValuesIterator(elements);
        }

        @Override
        public IteratorSourceSplit<RowData, TestValuesIterator> getUpdatedSplitForIterator(
                TestValuesIterator iterator) {
            return new TestValuesSplit(splitId, elements);
        }

        @Override
        public String splitId() {
            return splitId;
        }
    }

    /** A source Reader for {@link TestValuesSource}. */
    private static class TestValuesReader
            extends IteratorSourceReader<RowData, TestValuesIterator, TestValuesSplit> {

        public TestValuesReader(SourceReaderContext context) {
            super(context);
        }

        @Override
        public InputStatus pollNext(ReaderOutput<RowData> output) {
            InputStatus status = super.pollNext(output);
            if (InputStatus.MORE_AVAILABLE.equals(status)) {
                ((SourceOutputWithWatermarks<RowData>) output).emitPeriodicWatermark();
            }
            return status;
        }
    }

    /** A source elements iterator in {@link TestValuesSource}. */
    private static class TestValuesIterator extends SplittableIterator<RowData> {

        private final Iterator<RowData> elements;

        public TestValuesIterator(Iterator<RowData> elements) {
            this.elements = elements;
        }

        @Override
        public TestValuesIterator[] split(int numPartitions) {
            if (numPartitions < 1) {
                throw new IllegalArgumentException("The number of partitions must be at least 1.");
            }

            if (numPartitions == 1) {
                return new TestValuesIterator[] {new TestValuesIterator(elements)};
            }

            // As a test source, temporarily we don't care about splitting the source.
            // Only the first partition has the entire elements,
            // other partitions have the empty elements
            TestValuesIterator[] iters = new TestValuesIterator[numPartitions];
            iters[0] = new TestValuesIterator(elements);
            for (int j = 0; j < numPartitions; j++) {
                iters[j] = new TestValuesIterator(new ArrayList<RowData>().iterator());
            }
            return iters;
        }

        @Override
        public int getMaximumNumberOfSplits() {
            return IteratorUtils.toList(elements).size();
        }

        @Override
        public boolean hasNext() {
            return elements.hasNext();
        }

        @Override
        public RowData next() {
            return elements.next();
        }

        public Iterator<RowData> getElements() {
            return elements;
        }
    }

    /** A element serializer for {@link TestValuesSource}. */
    private static class TestValuesSplitSerializer
            implements SimpleVersionedSerializer<TestValuesSplit> {

        private static final int CURRENT_VERSION = 1;

        private final TypeSerializer<RowData> serializer;

        public TestValuesSplitSerializer(TypeSerializer<RowData> serializer) {
            this.serializer = serializer;
        }

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(TestValuesSplit split) throws IOException {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(out);
            serializeSplit(wrapper, split, serializer);

            return out.toByteArray();
        }

        public static void serializeSplit(
                DataOutputViewStreamWrapper wrapper,
                TestValuesSplit split,
                TypeSerializer<RowData> serializer)
                throws IOException {

            wrapper.writeUTF(split.splitId());
            List<RowData> elements = IteratorUtils.toList(split.getIterator());
            wrapper.writeInt(elements.size());
            Iterator<RowData> iterator = elements.iterator();

            while (iterator.hasNext()) {
                RowData element = iterator.next();
                serializer.serialize(element, wrapper);
            }
        }

        @Override
        public TestValuesSplit deserialize(int version, byte[] serialized) throws IOException {
            if (version != CURRENT_VERSION) {
                throw new IOException("Unrecognized version: " + version);
            }
            final ByteArrayInputStream in = new ByteArrayInputStream(serialized);
            final DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(in);

            return deserializeSplit(wrapper, serializer);
        }

        public static TestValuesSplit deserializeSplit(
                DataInputViewStreamWrapper wrapper, TypeSerializer<RowData> serializer)
                throws IOException {

            final String splitId = wrapper.readUTF();
            final int count = wrapper.readInt();
            final List<RowData> elements = new ArrayList<>();
            int index = 0;
            while (index < count) {
                elements.add(serializer.deserialize(wrapper));
                index++;
            }

            return new TestValuesSplit(splitId, elements.iterator());
        }
    }

    /** An enumerator serializer for {@link TestValuesSource}. */
    private static class TestValuesSourceEnumeratorSerializer
            implements SimpleVersionedSerializer<Collection<TestValuesSplit>> {

        private static final int CURRENT_VERSION = 1;

        private final TypeSerializer<RowData> serializer;

        public TestValuesSourceEnumeratorSerializer(TypeSerializer<RowData> serializer) {
            this.serializer = serializer;
        }

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(Collection<TestValuesSplit> splits) throws IOException {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(out);
            wrapper.writeInt(splits.size());
            for (TestValuesSplit split : splits) {
                TestValuesSplitSerializer.serializeSplit(wrapper, split, serializer);
            }

            return out.toByteArray();
        }

        @Override
        public Collection<TestValuesSplit> deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != CURRENT_VERSION) {
                throw new IOException("Unrecognized version: " + version);
            }
            final ByteArrayInputStream in = new ByteArrayInputStream(serialized);
            final DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(in);
            final int num = wrapper.readInt();
            final List<TestValuesSplit> result = new ArrayList<>();
            for (int i = 0; i < num; i++) {
                result.add(TestValuesSplitSerializer.deserializeSplit(wrapper, serializer));
            }

            return result;
        }
    }
}
