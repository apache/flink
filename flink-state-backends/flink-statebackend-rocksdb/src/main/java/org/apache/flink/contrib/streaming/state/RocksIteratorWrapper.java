/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.util.FlinkRuntimeException;

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksIteratorInterface;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.nio.ByteBuffer;

/**
 * This class was originally a wrapper around {@link RocksIterator} to check the iterator status for
 * all the methods mentioned to require this check in the wiki documentation: seek, next,
 * seekToFirst, seekToLast, seekForPrev, and prev. At that time, this was required because the
 * iterator may pass the blocks or files it had difficulties in reading (because of IO errors, data
 * corruptions or other issues) and continue with the next available keys. The status flag may not
 * be OK, even if the iterator is valid.
 *
 * <p>However, after <a href="https://github.com/facebook/rocksdb/pull/3810">3810</a> was merged,
 * the behaviour had changed. If the iterator is valid, the status() is guaranteed to be OK; If the
 * iterator is not valid, there are two possibilities: 1) We have reached the end of the data. And
 * in this case, status() is OK; 2) There is an error. In this case, status() is not OK; More
 * information can be found <a
 * href="https://github.com/facebook/rocksdb/wiki/Iterator#error-handling">here</a>.
 */
public class RocksIteratorWrapper implements RocksIteratorInterface, Closeable {

    private RocksIterator iterator;

    public RocksIteratorWrapper(@Nonnull RocksIterator iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean isValid() {
        boolean isValid = this.iterator.isValid();
        if (!isValid) {
            status();
        }

        return isValid;
    }

    @Override
    public void seekToFirst() {
        iterator.seekToFirst();
    }

    @Override
    public void seekToLast() {
        iterator.seekToLast();
    }

    @Override
    public void seek(byte[] target) {
        iterator.seek(target);
    }

    @Override
    public void seekForPrev(byte[] target) {
        iterator.seekForPrev(target);
    }

    @Override
    public void seek(ByteBuffer target) {
        iterator.seek(target);
    }

    @Override
    public void seekForPrev(ByteBuffer target) {
        iterator.seekForPrev(target);
    }

    @Override
    public void next() {
        iterator.next();
    }

    @Override
    public void prev() {
        iterator.prev();
    }

    @Override
    public void status() {
        try {
            iterator.status();
        } catch (RocksDBException ex) {
            throw new FlinkRuntimeException("Internal exception found in RocksDB", ex);
        }
    }

    @Override
    public void refresh() throws RocksDBException {
        iterator.refresh();
        status();
    }

    public byte[] key() {
        return iterator.key();
    }

    public byte[] value() {
        return iterator.value();
    }

    @Override
    public void close() {
        iterator.close();
    }
}
