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

package org.apache.flink.contrib.streaming.state.iterator;

import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.runtime.state.StateSnapshotTransformer;

import org.rocksdb.RocksIterator;

import javax.annotation.Nonnull;

/**
 * Wrapper around {@link RocksIterator} that applies a given {@link StateSnapshotTransformer} to the
 * elements during the iteration.
 */
public class RocksTransformingIteratorWrapper extends RocksIteratorWrapper {

    @Nonnull private final StateSnapshotTransformer<byte[]> stateSnapshotTransformer;
    private byte[] current;

    public RocksTransformingIteratorWrapper(
            @Nonnull RocksIterator iterator,
            @Nonnull StateSnapshotTransformer<byte[]> stateSnapshotTransformer) {
        super(iterator);
        this.stateSnapshotTransformer = stateSnapshotTransformer;
    }

    @Override
    public void seekToFirst() {
        super.seekToFirst();
        filterOrTransform(super::next);
    }

    @Override
    public void seekToLast() {
        super.seekToLast();
        filterOrTransform(super::prev);
    }

    @Override
    public void next() {
        super.next();
        filterOrTransform(super::next);
    }

    @Override
    public void prev() {
        super.prev();
        filterOrTransform(super::prev);
    }

    private void filterOrTransform(@Nonnull Runnable advance) {
        while (isValid()
                && (current = stateSnapshotTransformer.filterOrTransform(super.value())) == null) {
            advance.run();
        }
    }

    @Override
    public byte[] value() {
        if (!isValid()) {
            throw new IllegalStateException(
                    "value() method cannot be called if isValid() is false");
        }
        return current;
    }
}
