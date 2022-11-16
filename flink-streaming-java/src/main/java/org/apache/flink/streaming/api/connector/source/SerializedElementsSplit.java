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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/** A split of the {@link CollectionSource}, contains serialized part of collection. */
@Internal
public class SerializedElementsSplit<E>
        implements IteratorSourceSplit<E, Iterator<E>>, Serializable {
    private static final String SPLIT_ID = "0";

    private final byte[] serializedData;
    private final TypeSerializer<E> serializer;
    private final int elementsCount;

    private int currentOffset;

    public SerializedElementsSplit(
            byte[] serializedData, int elementsCount, TypeSerializer<E> serializer) {
        this(serializedData, elementsCount, serializer, 0);
    }

    public SerializedElementsSplit(
            byte[] serializedData,
            int elementsCount,
            TypeSerializer<E> serializer,
            int currentOffset) {
        this.serializer = serializer;
        this.elementsCount = elementsCount;
        this.serializedData = serializedData;
        this.currentOffset = currentOffset;
    }

    @Override
    public String splitId() {
        return SPLIT_ID;
    }

    @Override
    public Iterator<E> getIterator() {
        return new ElementsSplitIterator(currentOffset);
    }

    @Override
    public IteratorSourceSplit<E, Iterator<E>> getUpdatedSplitForIterator(Iterator<E> iterator) {
        return new SerializedElementsSplit<>(
                serializedData, elementsCount, serializer, currentOffset);
    }

    public int getCurrentOffset() {
        return currentOffset;
    }

    public byte[] getSerializedData() {
        return serializedData;
    }

    public TypeSerializer<E> getSerializer() {
        return serializer;
    }

    public int getElementsCount() {
        return elementsCount;
    }

    /** Iterates through serializedElements, lazily deserializing them. */
    @Internal
    public class ElementsSplitIterator implements Iterator<E> {
        private final DataInputViewStreamWrapper serializedElementsStream;

        public ElementsSplitIterator(int currentOffset) {
            this.serializedElementsStream =
                    new DataInputViewStreamWrapper(new ByteArrayInputStream(serializedData));
            skipElements(currentOffset);
        }

        @Override
        public boolean hasNext() {
            return currentOffset < elementsCount;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                E record = serializer.deserialize(serializedElementsStream);
                ++currentOffset;
                return record;
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize an element from the source.", e);
            }
        }

        private void skipElements(int elementsToSkip) {
            int toSkip = elementsToSkip;
            try {
                serializedElementsStream.reset();
                while (toSkip > 0) {
                    serializer.deserialize(serializedElementsStream);
                    toSkip--;
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize an element from the source.", e);
            }
        }

        public int getCurrentOffset() {
            return currentOffset;
        }
    }
}
