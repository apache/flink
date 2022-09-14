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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.primitives.UnsignedBytes;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import static org.apache.flink.contrib.streaming.state.RocksDBCachingPriorityQueueSet.OrderedByteArraySetCache.LEXICOGRAPHIC_BYTE_COMPARATOR;

/**
 * A priority queue with set semantics, implemented on top of RocksDB. This uses a {@link TreeSet}
 * to cache the bytes of up to the first n elements from RocksDB in memory to reduce interaction
 * with RocksDB, in particular seek operations. Cache uses a simple write-through policy.
 *
 * @param <E> the type of the contained elements in the queue.
 */
public class RocksDBCachingPriorityQueueSet<E extends HeapPriorityQueueElement>
        implements InternalPriorityQueue<E>, HeapPriorityQueueElement {

    /** Serialized empty value to insert into RocksDB. */
    private static final byte[] DUMMY_BYTES = new byte[] {};

    /** The RocksDB instance that serves as store. */
    @Nonnull private final RocksDB db;

    @Nonnull private final ReadOptions readOptions;

    /** Handle to the column family of the RocksDB instance in which the elements are stored. */
    @Nonnull private final ColumnFamilyHandle columnFamilyHandle;

    /**
     * Serializer for the contained elements. The lexicographical order of the bytes of serialized
     * objects must be aligned with their logical order.
     */
    @Nonnull private final TypeSerializer<E> byteOrderProducingSerializer;

    /** Wrapper to batch all writes to RocksDB. */
    @Nonnull private final RocksDBWriteBatchWrapper batchWrapper;

    /** The key-group id in serialized form. */
    @Nonnull private final byte[] groupPrefixBytes;

    /** Output view that helps to serialize elements. */
    @Nonnull private final DataOutputSerializer outputView;

    /** Input view that helps to de-serialize elements. */
    @Nonnull private final DataInputDeserializer inputView;

    /** In memory cache that holds a head-subset of the elements stored in RocksDB. */
    @Nonnull private final OrderedByteArraySetCache orderedCache;

    /**
     * This holds the key that we use to seek to the first element in RocksDB, to improve
     * seek/iterator performance.
     */
    @Nonnull private byte[] seekHint;

    /** Cache for the head element in de-serialized form. */
    @Nullable private E peekCache;

    /** This flag is true iff all elements in RocksDB are also contained in the cache. */
    private boolean allElementsInCache;

    /** Index for management as a {@link HeapPriorityQueueElement}. */
    private int internalIndex;

    RocksDBCachingPriorityQueueSet(
            @Nonnegative int keyGroupId,
            @Nonnegative int keyGroupPrefixBytes,
            @Nonnull RocksDB db,
            @Nonnull ReadOptions readOptions,
            @Nonnull ColumnFamilyHandle columnFamilyHandle,
            @Nonnull TypeSerializer<E> byteOrderProducingSerializer,
            @Nonnull DataOutputSerializer outputStream,
            @Nonnull DataInputDeserializer inputStream,
            @Nonnull RocksDBWriteBatchWrapper batchWrapper,
            @Nonnull OrderedByteArraySetCache orderedByteArraySetCache) {
        this.db = db;
        this.readOptions = readOptions;
        this.columnFamilyHandle = columnFamilyHandle;
        this.byteOrderProducingSerializer = byteOrderProducingSerializer;
        this.batchWrapper = batchWrapper;
        this.outputView = outputStream;
        this.inputView = inputStream;
        this.orderedCache = orderedByteArraySetCache;
        this.allElementsInCache = false;
        this.groupPrefixBytes = createKeyGroupBytes(keyGroupId, keyGroupPrefixBytes);
        this.seekHint = groupPrefixBytes;
        this.internalIndex = HeapPriorityQueueElement.NOT_CONTAINED;
    }

    @Nullable
    @Override
    public E peek() {

        checkRefillCacheFromStore();

        if (peekCache != null) {
            return peekCache;
        }

        byte[] firstBytes = orderedCache.peekFirst();
        if (firstBytes != null) {
            peekCache = deserializeElement(firstBytes);
            return peekCache;
        } else {
            return null;
        }
    }

    @Nullable
    @Override
    public E poll() {

        checkRefillCacheFromStore();

        final byte[] firstBytes = orderedCache.pollFirst();

        if (firstBytes == null) {
            return null;
        }

        // write-through sync
        removeFromRocksDB(firstBytes);

        if (orderedCache.isEmpty()) {
            seekHint = firstBytes;
        }

        if (peekCache != null) {
            E fromCache = peekCache;
            peekCache = null;
            return fromCache;
        } else {
            return deserializeElement(firstBytes);
        }
    }

    @Override
    public boolean add(@Nonnull E toAdd) {

        checkRefillCacheFromStore();

        final byte[] toAddBytes = serializeElement(toAdd);

        final boolean cacheFull = orderedCache.isFull();

        if ((!cacheFull && allElementsInCache)
                || LEXICOGRAPHIC_BYTE_COMPARATOR.compare(toAddBytes, orderedCache.peekLast()) < 0) {

            if (cacheFull) {
                // we drop the element with lowest priority from the cache
                orderedCache.pollLast();
                // the dropped element is now only in the store
                allElementsInCache = false;
            }

            if (orderedCache.add(toAddBytes)) {
                // write-through sync
                addToRocksDB(toAddBytes);
                if (toAddBytes == orderedCache.peekFirst()) {
                    peekCache = null;
                    return true;
                }
            }
        } else {
            // we only added to the store
            addToRocksDB(toAddBytes);
            allElementsInCache = false;
        }
        return false;
    }

    @Override
    public boolean remove(@Nonnull E toRemove) {

        checkRefillCacheFromStore();

        final byte[] oldHead = orderedCache.peekFirst();

        if (oldHead == null) {
            return false;
        }

        final byte[] toRemoveBytes = serializeElement(toRemove);

        // write-through sync
        removeFromRocksDB(toRemoveBytes);
        orderedCache.remove(toRemoveBytes);

        if (orderedCache.isEmpty()) {
            seekHint = toRemoveBytes;
            peekCache = null;
            return true;
        }

        if (oldHead != orderedCache.peekFirst()) {
            peekCache = null;
            return true;
        }

        return false;
    }

    @Override
    public void addAll(@Nullable Collection<? extends E> toAdd) {

        if (toAdd == null) {
            return;
        }

        for (E element : toAdd) {
            add(element);
        }
    }

    @Override
    public boolean isEmpty() {
        checkRefillCacheFromStore();
        return orderedCache.isEmpty();
    }

    @Nonnull
    @Override
    public CloseableIterator<E> iterator() {
        return new DeserializingIteratorWrapper(orderedBytesIterator());
    }

    /**
     * This implementation comes at a relatively high cost per invocation. It should not be called
     * repeatedly when it is clear that the value did not change. Currently this is only truly used
     * to realize certain higher-level tests.
     */
    @Override
    public int size() {

        if (allElementsInCache) {
            return orderedCache.size();
        } else {
            int count = 0;
            try (final RocksBytesIterator iterator = orderedBytesIterator()) {
                while (iterator.hasNext()) {
                    iterator.next();
                    ++count;
                }
            }
            return count;
        }
    }

    @Override
    public int getInternalIndex() {
        return internalIndex;
    }

    @Override
    public void setInternalIndex(int newIndex) {
        this.internalIndex = newIndex;
    }

    @Nonnull
    private RocksBytesIterator orderedBytesIterator() {
        flushWriteBatch();
        return new RocksBytesIterator(
                new RocksIteratorWrapper(db.newIterator(columnFamilyHandle, readOptions)));
    }

    /** Ensures that recent writes are flushed and reflect in the RocksDB instance. */
    private void flushWriteBatch() {
        try {
            batchWrapper.flush();
        } catch (RocksDBException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    private void addToRocksDB(@Nonnull byte[] toAddBytes) {
        try {
            batchWrapper.put(columnFamilyHandle, toAddBytes, DUMMY_BYTES);
        } catch (RocksDBException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    private void removeFromRocksDB(@Nonnull byte[] toRemoveBytes) {
        try {
            batchWrapper.remove(columnFamilyHandle, toRemoveBytes);
        } catch (RocksDBException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    private void checkRefillCacheFromStore() {
        if (!allElementsInCache && orderedCache.isEmpty()) {
            try (final RocksBytesIterator iterator = orderedBytesIterator()) {
                orderedCache.bulkLoadFromOrderedIterator(iterator);
                allElementsInCache = !iterator.hasNext();
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Exception while refilling store from iterator.", e);
            }
        }
    }

    private static boolean isPrefixWith(byte[] bytes, byte[] prefixBytes) {
        for (int i = 0; i < prefixBytes.length; ++i) {
            if (bytes[i] != prefixBytes[i]) {
                return false;
            }
        }
        return true;
    }

    @Nonnull
    private byte[] createKeyGroupBytes(int keyGroupId, int numPrefixBytes) {

        outputView.clear();

        try {
            CompositeKeySerializationUtils.writeKeyGroup(keyGroupId, numPrefixBytes, outputView);
        } catch (IOException e) {
            throw new FlinkRuntimeException("Could not write key-group bytes.", e);
        }

        return outputView.getCopyOfBuffer();
    }

    @Nonnull
    private byte[] serializeElement(@Nonnull E element) {
        try {
            outputView.clear();
            outputView.write(groupPrefixBytes);
            byteOrderProducingSerializer.serialize(element, outputView);
            return outputView.getCopyOfBuffer();
        } catch (IOException e) {
            throw new FlinkRuntimeException("Error while serializing the element.", e);
        }
    }

    @Nonnull
    private E deserializeElement(@Nonnull byte[] bytes) {
        try {
            final int numPrefixBytes = groupPrefixBytes.length;
            inputView.setBuffer(bytes, numPrefixBytes, bytes.length - numPrefixBytes);
            return byteOrderProducingSerializer.deserialize(inputView);
        } catch (IOException e) {
            throw new FlinkRuntimeException("Error while deserializing the element.", e);
        }
    }

    /**
     * Wraps an iterator over byte-arrays with deserialization logic, so that it iterates over
     * deserialized objects.
     */
    private class DeserializingIteratorWrapper implements CloseableIterator<E> {

        /** The iterator over byte-arrays with the serialized objects. */
        @Nonnull private final CloseableIterator<byte[]> bytesIterator;

        private DeserializingIteratorWrapper(@Nonnull CloseableIterator<byte[]> bytesIterator) {
            this.bytesIterator = bytesIterator;
        }

        @Override
        public void close() throws Exception {
            bytesIterator.close();
        }

        @Override
        public boolean hasNext() {
            return bytesIterator.hasNext();
        }

        @Override
        public E next() {
            return deserializeElement(bytesIterator.next());
        }
    }

    /**
     * Adapter between RocksDB iterator and Java iterator. This is also closeable to release the
     * native resources after use.
     */
    private class RocksBytesIterator implements CloseableIterator<byte[]> {

        /** The RocksDb iterator to which we forward ops. */
        @Nonnull private final RocksIteratorWrapper iterator;

        /** Cache for the current element of the iteration. */
        @Nullable private byte[] currentElement;

        private RocksBytesIterator(@Nonnull RocksIteratorWrapper iterator) {
            this.iterator = iterator;
            try {
                // We use our knowledge about the lower bound to issue a seek that is as close to
                // the first element in
                // the key-group as possible, i.e. we generate the next possible key after seekHint
                // by appending one
                // zero-byte.
                iterator.seek(Arrays.copyOf(seekHint, seekHint.length + 1));
                currentElement = nextElementIfAvailable();
            } catch (Exception ex) {
                // ensure resource cleanup also in the face of (runtime) exceptions in the
                // constructor.
                iterator.close();
                throw new FlinkRuntimeException("Could not initialize ordered iterator.", ex);
            }
        }

        @Override
        public void close() {
            iterator.close();
        }

        @Override
        public boolean hasNext() {
            return currentElement != null;
        }

        @Override
        public byte[] next() {
            final byte[] returnElement = this.currentElement;
            if (returnElement == null) {
                throw new NoSuchElementException("Iterator has no more elements!");
            }
            iterator.next();
            currentElement = nextElementIfAvailable();
            return returnElement;
        }

        private byte[] nextElementIfAvailable() {
            final byte[] elementBytes;
            return iterator.isValid()
                            && isPrefixWith((elementBytes = iterator.key()), groupPrefixBytes)
                    ? elementBytes
                    : null;
        }
    }

    /**
     * Cache that is organized as an ordered set for byte-arrays. The byte-arrays are sorted in
     * lexicographic order of their content. Caches typically have a bounded size.
     */
    public interface OrderedByteArraySetCache {

        /** Comparator for byte arrays. */
        Comparator<byte[]> LEXICOGRAPHIC_BYTE_COMPARATOR =
                UnsignedBytes.lexicographicalComparator();

        /** Returns the number of contained elements. */
        int size();

        /** Returns the maximum number of elements that can be stored in the cache. */
        int maxSize();

        /** Returns <code>size() == 0</code>. */
        boolean isEmpty();

        /** Returns <code>size() == maxSize()</code>. */
        boolean isFull();

        /**
         * Adds the given element, if it was not already contained. Returns <code>true</code> iff
         * the cache was modified.
         */
        boolean add(@Nonnull byte[] toAdd);

        /**
         * Removes the given element, if it is contained. Returns <code>true</code> iff the cache
         * was modified.
         */
        boolean remove(@Nonnull byte[] toRemove);

        /** Returns the first element or <code>null</code> if empty. */
        @Nullable
        byte[] peekFirst();

        /** Returns the last element or <code>null</code> if empty. */
        @Nullable
        byte[] peekLast();

        /** Returns and removes the first element or returns <code>null</code> if empty. */
        @Nullable
        byte[] pollFirst();

        /** Returns and removes the last element or returns <code>null</code> if empty. */
        @Nullable
        byte[] pollLast();

        /**
         * Clears the cache and adds up to <code>maxSize()</code> elements from the iterator to the
         * cache. Iterator must be ordered in the same order as this cache.
         *
         * @param orderedIterator iterator with elements in-order.
         */
        void bulkLoadFromOrderedIterator(@Nonnull Iterator<byte[]> orderedIterator);
    }
}
