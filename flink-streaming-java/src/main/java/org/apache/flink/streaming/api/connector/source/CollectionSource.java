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

package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.util.serialization.CollectionSerializer;
import org.apache.flink.streaming.util.serialization.SimpleObjectSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * A {@link Source} implementation that reads data from a collection.
 *
 * <p>This source serializes the elements using Flink's type information. That way, any object
 * transport using Java serialization will not be affected by the serializability of the elements.
 *
 * <p>Note: Parallelism of this source must be 1.
 */
@Internal
public class CollectionSource<E>
        implements Source<E, SerializedElementsSplit<E>, Collection<SerializedElementsSplit<E>>>,
                OutputTypeConfigurable<E> {
    private final transient Iterable<E> elements;
    private final int elementsCount;

    private byte[] serializedElements;
    private TypeSerializer<E> serializer;

    public CollectionSource(Iterable<E> elements, TypeSerializer<E> serializer) {
        this.elements = Preconditions.checkNotNull(elements);
        this.serializer = Preconditions.checkNotNull(serializer);
        this.elementsCount = Iterables.size(elements);
        checkIterable(elements, Object.class);
        serializeElements();
    }

    public CollectionSource(Iterable<E> elements) {
        this.elements = Preconditions.checkNotNull(elements);
        this.elementsCount = Iterables.size(elements);
        checkIterable(elements, Object.class);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<E, SerializedElementsSplit<E>> createReader(
            SourceReaderContext readerContext) throws Exception {
        return new IteratorSourceReader<>(readerContext);
    }

    @Override
    public SplitEnumerator<SerializedElementsSplit<E>, Collection<SerializedElementsSplit<E>>>
            createEnumerator(SplitEnumeratorContext<SerializedElementsSplit<E>> enumContext)
                    throws Exception {
        SerializedElementsSplit<E> split =
                new SerializedElementsSplit<>(serializedElements, elementsCount, serializer);
        return new IteratorSourceEnumerator<>(enumContext, Collections.singletonList(split));
    }

    @Override
    public SplitEnumerator<SerializedElementsSplit<E>, Collection<SerializedElementsSplit<E>>>
            restoreEnumerator(
                    SplitEnumeratorContext<SerializedElementsSplit<E>> enumContext,
                    Collection<SerializedElementsSplit<E>> restoredSplits)
                    throws Exception {
        Preconditions.checkArgument(
                restoredSplits.size() <= 1, "Parallelism of CollectionSource should be 1");
        return new IteratorSourceEnumerator<>(enumContext, restoredSplits);
    }

    @Override
    public SimpleVersionedSerializer<SerializedElementsSplit<E>> getSplitSerializer() {
        return new ElementsSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<SerializedElementsSplit<E>>>
            getEnumeratorCheckpointSerializer() {
        return new CollectionSerializer<>(new ElementsSplitSerializer());
    }

    private void serializeElements() {
        Preconditions.checkState(serializer != null, "serializer not set");
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos)) {
            for (E element : elements) {
                serializer.serialize(element, wrapper);
            }
            this.serializedElements = baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Serializing the source elements failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void setOutputType(TypeInformation<E> outTypeInfo, ExecutionConfig executionConfig) {
        Preconditions.checkState(
                outTypeInfo != null,
                "The output type should've been specified before shipping the graph to the cluster");
        checkIterable(elements, outTypeInfo.getTypeClass());
        TypeSerializer<E> newSerializer = outTypeInfo.createSerializer(executionConfig);
        if (Objects.equals(serializer, newSerializer)) {
            return;
        }
        serializer = newSerializer;
        serializeElements();
    }

    @VisibleForTesting
    byte[] getSerializedElements() {
        return serializedElements;
    }

    @VisibleForTesting
    @Nullable
    public TypeSerializer<E> getSerializer() {
        return serializer;
    }

    /** {@link SerializedElementsSplit} serializer. */
    @Internal
    public class ElementsSplitSerializer
            extends SimpleObjectSerializer<SerializedElementsSplit<E>> {
        static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public void serialize(SerializedElementsSplit<E> obj, ObjectOutputStream outputStream)
                throws IOException {
            byte[] serializedData = obj.getSerializedData();
            outputStream.writeInt(serializedData.length);
            outputStream.write(serializedData);
            outputStream.writeInt(obj.getElementsCount());
            outputStream.writeInt(obj.getCurrentOffset());
        }

        @Override
        public SerializedElementsSplit<E> deserialize(ObjectInputStream inputStream)
                throws IOException, ClassNotFoundException {
            int buffSize = inputStream.readInt();
            byte[] serializedData = new byte[buffSize];
            inputStream.readFully(serializedData);
            int elementsCount = inputStream.readInt();
            int currentOffset = inputStream.readInt();
            return new SerializedElementsSplit<>(
                    serializedData, elementsCount, serializer, currentOffset);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Verifies that all elements in the collection are non-null, and are of the given class, or a
     * subclass thereof.
     *
     * @param elements The collection to check.
     * @param viewedAs The class to which the elements must be assignable to.
     * @param <OUT> The generic type of the collection to be checked.
     */
    public static <OUT> void checkIterable(Iterable<OUT> elements, Class<?> viewedAs) {
        for (OUT elem : elements) {
            if (elem == null) {
                throw new IllegalArgumentException("The collection contains a null element");
            }

            if (!viewedAs.isAssignableFrom(elem.getClass())) {
                throw new IllegalArgumentException(
                        "The elements in the collection are not all subclasses of "
                                + viewedAs.getCanonicalName());
            }
        }
    }
}
