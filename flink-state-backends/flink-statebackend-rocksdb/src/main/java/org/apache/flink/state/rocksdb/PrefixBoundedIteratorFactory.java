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

package org.apache.flink.state.rocksdb;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DirectSlice;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;

/**
 * Creates prefix-bounded iterators for MapState scans. RocksDB borrows a pointer to the upper bound
 * for the lifetime of the iterator. Once that iterator closes, the next scan can reuse its slice.
 * Overlapping iterators and bounds that exceed the buffer capacity get a slice of their own.
 */
final class PrefixBoundedIteratorFactory implements AutoCloseable {

    @VisibleForTesting static final int MAX_REUSED_BOUND_LENGTH = 256;

    private final ReadOptions readOptions;
    @Nullable private ByteBuffer reusedBound;
    @Nullable private DirectSlice reusedBoundSlice;
    @Nullable private ReusedBoundIterator boundOwner;
    private boolean directMemoryUnavailable;
    private boolean closed;

    PrefixBoundedIteratorFactory(ReadOptions readOptions) {
        this.readOptions = readOptions;
    }

    synchronized RocksIteratorWrapper newIterator(
            RocksDB db, ColumnFamilyHandle columnFamilyHandle, byte[] prefix) {
        Preconditions.checkState(!closed, "The iterator factory is closed.");
        final int length = RocksDBOperationUtils.getPrefixEndLength(prefix);
        if (length == 0
                || boundOwner != null
                || length > MAX_REUSED_BOUND_LENGTH
                || !ensureReusedBound(length)) {
            return RocksDBOperationUtils.getRocksIteratorBoundedByPrefix(
                    db, columnFamilyHandle, readOptions, prefix, length);
        }

        if (length != reusedBound.limit()) {
            reusedBoundSlice.setLength(length);
            reusedBound.limit(length);
        }
        reusedBound.rewind();
        reusedBound.put(prefix, 0, length - 1);
        reusedBound.put((byte) (prefix[length - 1] + 1));

        final ReadOptions options =
                RocksDBOperationUtils.copyForPrefixBoundedIterators(readOptions, reusedBoundSlice);
        try {
            boundOwner =
                    new ReusedBoundIterator(db.newIterator(columnFamilyHandle, options), options);
            return boundOwner;
        } catch (RuntimeException | Error e) {
            IOUtils.closeQuietly(options);
            throw e;
        }
    }

    private boolean ensureReusedBound(int length) {
        if (reusedBound != null) {
            return true;
        }
        if (directMemoryUnavailable) {
            return false;
        }
        final ByteBuffer buffer;
        try {
            buffer = ByteBuffer.allocateDirect(MAX_REUSED_BOUND_LENGTH);
        } catch (OutOfMemoryError e) {
            if (!ExceptionUtils.isDirectOutOfMemoryError(e)) {
                throw e;
            }
            // A private RocksDB slice does not use the JVM's direct-buffer allowance.
            directMemoryUnavailable = true;
            return false;
        }
        buffer.limit(length);
        reusedBoundSlice = new DirectSlice(buffer, length);
        reusedBound = buffer;
        return true;
    }

    @VisibleForTesting
    synchronized boolean isReusedBoundInUse() {
        return boundOwner != null;
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (boundOwner == null) {
            IOUtils.closeQuietly(reusedBoundSlice);
            reusedBoundSlice = null;
            reusedBound = null;
        }
    }

    private final class ReusedBoundIterator extends RocksIteratorWrapper {
        private ReusedBoundIterator(RocksIterator iterator, ReadOptions options) {
            super(iterator, options, null);
        }

        @Override
        public void close() {
            synchronized (PrefixBoundedIteratorFactory.this) {
                if (boundOwner == this) {
                    super.close();
                    boundOwner = null;
                    if (closed) {
                        PrefixBoundedIteratorFactory.this.close();
                    }
                }
            }
        }
    }
}
