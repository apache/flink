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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.util.FileUtils;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;
import static org.junit.Assert.fail;

/** {@link RocksDBStateFileVerifier} test. */
public class RocksdbStateFileVerifierTest {

    @TempDir java.nio.file.Path folder;

    @Test
    public void rocksdbStateFileVerifierTest() throws Exception {
        List columnFamilyHandles = new ArrayList<>(1);
        String rootPath = folder.toAbsolutePath().toString();
        File dbPath = new File(rootPath, "db");
        File cpPath = new File(rootPath, "cp");

        try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
                ColumnFamilyOptions colOptions = new ColumnFamilyOptions();
                Options sstFileReaderOptions = new Options(dbOptions, colOptions);
                WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);
                RocksDB db =
                        RocksDB.open(
                                dbOptions,
                                dbPath.toString(),
                                Collections.singletonList(
                                        new ColumnFamilyDescriptor(
                                                "default".getBytes(), colOptions)),
                                columnFamilyHandles);
                RocksDBStateFileVerifier rocksDBStateFileVerifier =
                        new RocksDBStateFileVerifier(sstFileReaderOptions)) {

            byte[] key = "checkpoint".getBytes();
            byte[] val = "incrementalTest".getBytes();
            db.put(writeOptions, key, val);

            try (Checkpoint checkpoint = Checkpoint.create(db)) {
                checkpoint.createCheckpoint(cpPath.toString());
            }

            List<Path> sstFiles =
                    Arrays.stream(FileUtils.listDirectory(cpPath.toPath()))
                            .filter(file -> file.getFileName().toString().endsWith(SST_FILE_SUFFIX))
                            .collect(Collectors.toList());

            Assert.assertFalse(sstFiles.isEmpty());

            try {
                rocksDBStateFileVerifier.verifySstFilesChecksum(sstFiles);
            } catch (IOException e) {
                fail(e.getMessage());
            }
            // corrupt sst file.
            Path chosenSstFile = sstFiles.get(0);
            File corruptedSstFile =
                    new File(
                            chosenSstFile.getParent().toString(),
                            "corrupted_" + chosenSstFile.getFileName().toString());
            corruptSstFile(chosenSstFile, corruptedSstFile.toPath());
            sstFiles.add(corruptedSstFile.toPath());
            try {
                rocksDBStateFileVerifier.verifySstFilesChecksum(sstFiles);
                // corrupted sst file would verify failed
                fail("verifySstFilesChecksum should failed");
            } catch (IOException e) {
                Assertions.assertTrue(
                        e.getMessage().contains("Error while verifying Checksum of Sst File"));
            }
        }
    }

    public static void corruptSstFile(Path originalFile, Path corruptedFile) throws IOException {
        byte[] sstData = FileUtils.readAllBytes(originalFile);
        byte[] corruptedSstData = sstData.clone();
        int startIndex = corruptedSstData.length / 4;
        int endIndex = (corruptedSstData.length / 4) * 3;
        for (int i = startIndex; i < endIndex; i++) {
            corruptedSstData[i] ^= 0x80;
        }
        org.apache.commons.io.FileUtils.writeByteArrayToFile(
                corruptedFile.toFile(), corruptedSstData);
    }
}
