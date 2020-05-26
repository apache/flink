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
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Tests for {@link RocksDBSSTWriter}. */
public class RocksDBSSTWriterTest {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void basicTest() throws Exception {
        List<Tuple2<byte[], byte[]>> data = new ArrayList<>(10000);
        for (int i = 0; i < 10000; ++i) {
            data.add(
                    new Tuple2<>(
                            // Pad the key integer so data is in ascending key-order.
                            (String.format("key:%010d", i)).getBytes(), ("value:" + i).getBytes()));
        }

        File sstCreationFolder = folder.newFolder();
        File sstFile = new File(sstCreationFolder, "test.sst");

        try (RocksDB db = RocksDB.open(folder.newFolder().getAbsolutePath());
                WriteOptions options = new WriteOptions().setDisableWAL(true);
                IngestExternalFileOptions ingestOptions = new IngestExternalFileOptions();
                ColumnFamilyHandle handle =
                        db.createColumnFamily(new ColumnFamilyDescriptor("test".getBytes()));
                RocksDBSSTWriter writer = new RocksDBSSTWriter(null, null, handle, sstFile)) {

            // insert data
            for (Tuple2<byte[], byte[]> item : data) {
                writer.put(item.f0, item.f1);
            }
            writer.finish();

            // add the sst file to the RocksDB database
            List<String> sstFiles = Collections.singletonList(sstFile.getAbsolutePath());
            db.ingestExternalFile(handle, sstFiles, ingestOptions);

            // validate the results
            for (Tuple2<byte[], byte[]> item : data) {
                Assert.assertArrayEquals(item.f1, db.get(handle, item.f0));
            }
        }
    }
}
