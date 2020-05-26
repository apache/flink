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

package org.apache.flink.contrib.streaming.state.writer;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * {@link RocksDBWriter} is an interface for providing batch writing to RocksDB.
 *
 * <p>IMPORTANT: Implementations of this interface are implemented as thread-safe.
 */
public interface RocksDBWriter extends AutoCloseable {

    /**
     * Puts data into the underlying {@link org.rocksdb.RocksDB}.
     *
     * @param key to put
     * @param value to put
     * @throws RocksDBException
     */
    void put(
            @Nonnull ColumnFamilyHandle columnFamilyHandle,
            @Nonnull byte[] key,
            @Nonnull byte[] value)
            throws RocksDBException, IOException;

    /**
     * Flushes all data that has been written using {@link #put(ColumnFamilyHandle, byte[],
     * byte[])}.
     *
     * @throws RocksDBException
     */
    void flush() throws RocksDBException;

    /**
     * Overrides {@link AutoCloseable} to specify that only {@link RocksDBException} is thrown. This
     * also flushes in-progress data.
     *
     * @throws RocksDBException
     */
    @Override
    void close() throws RocksDBException;
}
