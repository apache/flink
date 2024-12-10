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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.execution.CancelTaskException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.WRITE_BATCH_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests to guard {@link RocksDBWriteBatchWrapper}. */
public class RocksDBWriteBatchWrapperTest {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test(expected = CancelTaskException.class)
    public void testAsyncCancellation() throws Exception {
        final CompletableFuture<Void> writeStartedFuture = new CompletableFuture<>();
        final CompletableFuture<Void> cancellationRequestedFuture = new CompletableFuture<>();
        final CloseableRegistry registry = new CloseableRegistry();
        new Thread(
                        () -> {
                            writeStartedFuture.join();
                            try {
                                registry.close();
                                cancellationRequestedFuture.complete(null);
                            } catch (IOException e) {
                                cancellationRequestedFuture.completeExceptionally(e);
                            }
                        })
                .start();

        final int capacity = 1000; // max
        final int cancellationCheckInterval = 1;
        long batchSizeBytes = WRITE_BATCH_SIZE.defaultValue().getBytes();

        try (RocksDB db = RocksDB.open(folder.newFolder().getAbsolutePath());
                WriteOptions options = new WriteOptions().setDisableWAL(true);
                ColumnFamilyHandle handle =
                        db.createColumnFamily(new ColumnFamilyDescriptor("test".getBytes()));
                RocksDBWriteBatchWrapper writeBatchWrapper =
                        new RocksDBWriteBatchWrapper(
                                db,
                                options,
                                capacity,
                                batchSizeBytes,
                                cancellationCheckInterval,
                                batchSizeBytes)) {
            registry.registerCloseable(writeBatchWrapper.getCancelCloseable());
            writeStartedFuture.complete(null);

            //noinspection InfiniteLoopStatement
            for (int i = 0; ; i++) {
                try {
                    writeBatchWrapper.put(
                            handle, ("key:" + i).getBytes(), ("value:" + i).getBytes());
                } catch (Exception e) {
                    cancellationRequestedFuture.join(); // shouldn't have any errors
                    throw e;
                }
                // make sure that cancellation is triggered earlier than periodic flush
                // but allow some delay of cancellation propagation
                assertThat(i).isLessThan(cancellationCheckInterval * 2);
            }
        }
    }

    @Test
    public void basicTest() throws Exception {

        List<Tuple2<byte[], byte[]>> data = new ArrayList<>(10000);
        for (int i = 0; i < 10000; ++i) {
            data.add(new Tuple2<>(("key:" + i).getBytes(), ("value:" + i).getBytes()));
        }

        try (RocksDB db = RocksDB.open(folder.newFolder().getAbsolutePath());
                WriteOptions options = new WriteOptions().setDisableWAL(true);
                ColumnFamilyHandle handle =
                        db.createColumnFamily(new ColumnFamilyDescriptor("test".getBytes()));
                RocksDBWriteBatchWrapper writeBatchWrapper =
                        new RocksDBWriteBatchWrapper(
                                db, options, 200, WRITE_BATCH_SIZE.defaultValue().getBytes())) {

            // insert data
            for (Tuple2<byte[], byte[]> item : data) {
                writeBatchWrapper.put(handle, item.f0, item.f1);
            }
            writeBatchWrapper.flush();

            // valid result
            for (Tuple2<byte[], byte[]> item : data) {
                Assert.assertArrayEquals(item.f1, db.get(handle, item.f0));
            }
        }
    }

    /**
     * Tests that {@link RocksDBWriteBatchWrapper} flushes after the memory consumed exceeds the
     * preconfigured value.
     */
    @Test
    public void testWriteBatchWrapperFlushAfterMemorySizeExceed() throws Exception {
        try (RocksDB db = RocksDB.open(folder.newFolder().getAbsolutePath());
                WriteOptions options = new WriteOptions().setDisableWAL(true);
                ColumnFamilyHandle handle =
                        db.createColumnFamily(new ColumnFamilyDescriptor("test".getBytes()));
                RocksDBWriteBatchWrapper writeBatchWrapper =
                        new RocksDBWriteBatchWrapper(db, options, 200, 50)) {

            long initBatchSize = writeBatchWrapper.getDataSize();
            byte[] dummy = new byte[6];
            ThreadLocalRandom.current().nextBytes(dummy);
            // will add 1 + 1 + 1 + 6 + 1 + 6 = 16 bytes for each KV
            // format is [handleType|kvType|keyLen|key|valueLen|value]
            // more information please ref write_batch.cc in RocksDB
            writeBatchWrapper.put(handle, dummy, dummy);
            assertEquals(initBatchSize + 16, writeBatchWrapper.getDataSize());
            writeBatchWrapper.put(handle, dummy, dummy);
            assertEquals(initBatchSize + 32, writeBatchWrapper.getDataSize());
            writeBatchWrapper.put(handle, dummy, dummy);
            // will flush all, then an empty write batch
            assertEquals(initBatchSize, writeBatchWrapper.getDataSize());
        }
    }

    /**
     * Tests that {@link RocksDBWriteBatchWrapper} flushes after the kv count exceeds the
     * preconfigured value.
     */
    @Test
    public void testWriteBatchWrapperFlushAfterCountExceed() throws Exception {
        try (RocksDB db = RocksDB.open(folder.newFolder().getAbsolutePath());
                WriteOptions options = new WriteOptions().setDisableWAL(true);
                ColumnFamilyHandle handle =
                        db.createColumnFamily(new ColumnFamilyDescriptor("test".getBytes()));
                RocksDBWriteBatchWrapper writeBatchWrapper =
                        new RocksDBWriteBatchWrapper(db, options, 100, 50000)) {
            long initBatchSize = writeBatchWrapper.getDataSize();
            byte[] dummy = new byte[2];
            ThreadLocalRandom.current().nextBytes(dummy);
            for (int i = 1; i < 100; ++i) {
                writeBatchWrapper.put(handle, dummy, dummy);
                // each kv consumes 8 bytes
                assertEquals(initBatchSize + 8 * i, writeBatchWrapper.getDataSize());
            }
            writeBatchWrapper.put(handle, dummy, dummy);
            assertEquals(initBatchSize, writeBatchWrapper.getDataSize());
        }
    }

    /**
     * Test that {@link RocksDBWriteBatchWrapper} creates default {@link WriteOptions} with disabled
     * WAL and closes them correctly.
     */
    @Test
    public void testDefaultWriteOptionsHaveDisabledWAL() throws Exception {
        WriteOptions options;
        try (RocksDB db = RocksDB.open(folder.newFolder().getAbsolutePath());
                RocksDBWriteBatchWrapper writeBatchWrapper =
                        new RocksDBWriteBatchWrapper(db, null, 200, 50)) {
            options = writeBatchWrapper.getOptions();
            assertTrue(options.isOwningHandle());
            assertTrue(options.disableWAL());
        }
        assertFalse(options.isOwningHandle());
    }

    /**
     * Test that {@link RocksDBWriteBatchWrapper} respects passed in {@link WriteOptions} and does
     * not close them.
     */
    @Test
    public void testNotClosingPassedInWriteOption() throws Exception {
        try (WriteOptions passInOption = new WriteOptions().setDisableWAL(false)) {
            try (RocksDB db = RocksDB.open(folder.newFolder().getAbsolutePath());
                    RocksDBWriteBatchWrapper writeBatchWrapper =
                            new RocksDBWriteBatchWrapper(db, passInOption, 200, 50)) {
                WriteOptions options = writeBatchWrapper.getOptions();
                assertTrue(options.isOwningHandle());
                assertFalse(options.disableWAL());
            }
            assertTrue(passInOption.isOwningHandle());
        }
    }
}
