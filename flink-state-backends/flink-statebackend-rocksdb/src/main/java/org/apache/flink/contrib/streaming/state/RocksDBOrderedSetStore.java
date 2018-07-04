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
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Implementation of {@link org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet.OrderedSetStore}
 * based on RocksDB.
 *
 * <p>IMPORTANT: The store is ordered and the order is determined by the lexicographic order of the byte sequences
 * produced by the provided serializer for the elements!
 *
 * @param <T> the type of stored elements.
 */
public class RocksDBOrderedSetStore<T> implements CachingInternalPriorityQueueSet.OrderedSetStore<T> {

	/** Serialized empty value to insert into RocksDB. */
	private static final byte[] DUMMY_BYTES = new byte[] {0};

	/** The RocksDB instance that serves as store. */
	@Nonnull
	private final RocksDB db;

	/** Handle to the column family of the RocksDB instance in which the elements are stored. */
	@Nonnull
	private final ColumnFamilyHandle columnFamilyHandle;

	/**
	 * Serializer for the contained elements. The lexicographical order of the bytes of serialized objects must be
	 * aligned with their logical order.
	 */
	@Nonnull
	private final TypeSerializer<T> byteOrderProducingSerializer;

	/** Wrapper to batch all writes to RocksDB. */
	@Nonnull
	private final RocksDBWriteBatchWrapper batchWrapper;

	/** The key-group id in serialized form. */
	@Nonnull
	private final byte[] groupPrefixBytes;

	/** Output stream that helps to serialize elements. */
	@Nonnull
	private final ByteArrayOutputStreamWithPos outputStream;

	/** Output view that helps to serialize elements, must wrap the output stream. */
	@Nonnull
	private final DataOutputViewStreamWrapper outputView;

	public RocksDBOrderedSetStore(
		@Nonnegative int keyGroupId,
		@Nonnegative int keyGroupPrefixBytes,
		@Nonnull RocksDB db,
		@Nonnull ColumnFamilyHandle columnFamilyHandle,
		@Nonnull TypeSerializer<T> byteOrderProducingSerializer,
		@Nonnull ByteArrayOutputStreamWithPos outputStream,
		@Nonnull DataOutputViewStreamWrapper outputView,
		@Nonnull RocksDBWriteBatchWrapper batchWrapper) {
		this.db = db;
		this.columnFamilyHandle = columnFamilyHandle;
		this.byteOrderProducingSerializer = byteOrderProducingSerializer;
		this.outputStream = outputStream;
		this.outputView = outputView;
		this.batchWrapper = batchWrapper;
		this.groupPrefixBytes = createKeyGroupBytes(keyGroupId, keyGroupPrefixBytes);
	}

	private byte[] createKeyGroupBytes(int keyGroupId, int numPrefixBytes) {

		outputStream.reset();

		try {
			RocksDBKeySerializationUtils.writeKeyGroup(keyGroupId, numPrefixBytes, outputView);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not write key-group bytes.", e);
		}

		return outputStream.toByteArray();
	}

	@Override
	public void add(@Nonnull T element) {
		byte[] elementBytes = serializeElement(element);
		try {
			batchWrapper.put(columnFamilyHandle, elementBytes, DUMMY_BYTES);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Error while getting element from RocksDB.", e);
		}
	}

	@Override
	public void remove(@Nonnull T element) {
		byte[] elementBytes = serializeElement(element);
		try {
			batchWrapper.remove(columnFamilyHandle, elementBytes);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Error while removing element from RocksDB.", e);
		}
	}

	/**
	 * This implementation comes at a relatively high cost per invocation. It should not be called repeatedly when it is
	 * clear that the value did not change. Currently this is only truly used to realize certain higher-level tests.
	 *
	 * @see org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet.OrderedSetStore
	 */
	@Override
	public int size() {

		int count = 0;
		try (final RocksToJavaIteratorAdapter iterator = orderedIterator()) {
			while (iterator.hasNext()) {
				iterator.next();
				++count;
			}
		}

		return count;
	}

	@Nonnull
	@Override
	public RocksToJavaIteratorAdapter orderedIterator() {

		flushWriteBatch();

		return new RocksToJavaIteratorAdapter(
			new RocksIteratorWrapper(
				db.newIterator(columnFamilyHandle)));
	}

	/**
	 * Ensures that recent writes are flushed and reflect in the RocksDB instance.
	 */
	private void flushWriteBatch() {
		try {
			batchWrapper.flush();
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException(e);
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

	private byte[] serializeElement(T element) {
		try {
			outputStream.reset();
			outputView.write(groupPrefixBytes);
			byteOrderProducingSerializer.serialize(element, outputView);
			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while serializing the element.", e);
		}
	}

	private T deserializeElement(byte[] bytes) {
		try {
			// TODO introduce a stream in which we can change the internal byte[] to avoid creating instances per call
			ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(bytes);
			DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
			inputView.skipBytes(groupPrefixBytes.length);
			return byteOrderProducingSerializer.deserialize(inputView);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while deserializing the element.", e);
		}
	}

	/**
	 * Adapter between RocksDB iterator and Java iterator. This is also closeable to release the native resources after
	 * use.
	 */
	private class RocksToJavaIteratorAdapter implements CloseableIterator<T> {

		/** The RocksDb iterator to which we forward ops. */
		@Nonnull
		private final RocksIteratorWrapper iterator;

		/** Cache for the current element of the iteration. */
		@Nullable
		private T currentElement;

		private RocksToJavaIteratorAdapter(@Nonnull RocksIteratorWrapper iterator) {
			this.iterator = iterator;
			try {
				// TODO we could check if it is more efficient to make the seek more specific, e.g. with a provided hint
				// that is lexicographically closer the first expected element in the key-group. I wonder if this could
				// help to improve the seek if there are many tombstones for elements at the beginning of the key-group
				// (like for elements that have been removed in previous polling, before they are compacted away).
				iterator.seek(groupPrefixBytes);
				deserializeNextElementIfAvailable();
			} catch (Exception ex) {
				// ensure resource cleanup also in the face of (runtime) exceptions in the constructor.
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
		public T next() {
			final T returnElement = this.currentElement;
			if (returnElement == null) {
				throw new NoSuchElementException("Iterator has no more elements!");
			}
			iterator.next();
			deserializeNextElementIfAvailable();
			return returnElement;
		}

		private void deserializeNextElementIfAvailable() {
			if (iterator.isValid()) {
				final byte[] elementBytes = iterator.key();
				if (isPrefixWith(elementBytes, groupPrefixBytes)) {
					this.currentElement = deserializeElement(elementBytes);
				} else {
					this.currentElement = null;
				}
			} else {
				this.currentElement = null;
			}
		}
	}
}
