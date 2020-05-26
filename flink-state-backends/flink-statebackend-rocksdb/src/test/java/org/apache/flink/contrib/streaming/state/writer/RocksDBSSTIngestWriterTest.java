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

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.List;

/** Tests for {@link RocksDBSSTIngestWriterTest}. */
public class RocksDBSSTIngestWriterTest {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Tests the basic functionality of {@link RocksDBSSTIngestWriter} for writing k/v to {@link
     * RocksDB}, including using multiple column families.
     *
     * @throws Exception
     */
    @Test
    public void basicTest() throws Exception {
        List<Tuple2<byte[], byte[]>> firstColumnFamilyData = new ArrayList<>(10000);
        for (int i = 0; i < 10000; ++i) {
            firstColumnFamilyData.add(
                    new Tuple2<>(
                            // Pad the key integer so data is in ascending key-order.
                            (String.format("family-1-key:%010d", i)).getBytes(),
                            ("value:" + i).getBytes()));
        }

        List<Tuple2<byte[], byte[]>> secondColumnFamilyData = new ArrayList<>(10000);
        for (int i = 0; i < 10000; ++i) {
            secondColumnFamilyData.add(
                    new Tuple2<>(
                            // Pad the key integer so data is in ascending key-order.
                            (String.format("family-2-key:%010d", i)).getBytes(),
                            ("value:" + i).getBytes()));
        }

        try (RocksDB db = RocksDB.open(folder.newFolder().getAbsolutePath());
                // @lgo: fixme: plumb through options and ingestOptions.
                WriteOptions options = new WriteOptions().setDisableWAL(true);
                ColumnFamilyHandle firstHandle =
                        db.createColumnFamily(
                                new ColumnFamilyDescriptor("test-handle-1".getBytes()));
                ColumnFamilyHandle secondHandle =
                        db.createColumnFamily(
                                new ColumnFamilyDescriptor("test-handle-2".getBytes()));
                RocksDBWriter writer =
                        new RocksDBSSTIngestWriter(db, 200, null, null, folder.newFolder())) {

            // insert data into the first column family.
            for (Tuple2<byte[], byte[]> item : firstColumnFamilyData) {
                writer.put(firstHandle, item.f0, item.f1);
            }

            // insert data into the second column family.
            for (Tuple2<byte[], byte[]> item : secondColumnFamilyData) {
                writer.put(secondHandle, item.f0, item.f1);
            }

            // flush all of the data
            writer.flush();

            // validate the results for the first column family.
            for (Tuple2<byte[], byte[]> item : firstColumnFamilyData) {
                Assert.assertArrayEquals(item.f1, db.get(firstHandle, item.f0));
            }

            // validate the results for the second column family.
            for (Tuple2<byte[], byte[]> item : secondColumnFamilyData) {
                Assert.assertArrayEquals(item.f1, db.get(secondHandle, item.f0));
            }
        }
    }
}
