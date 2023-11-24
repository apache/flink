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

import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/** Help class for verify Checksum RocksDB state files. */
public class RocksDBStateFileVerifier implements AutoCloseable {

    private final SstFileReader sstFileReader;

    public RocksDBStateFileVerifier(Options sstFileReaderOptions) {
        this.sstFileReader = new SstFileReader(sstFileReaderOptions);
    }

    public void verifySstFilesChecksum(@Nonnull List<Path> files) throws IOException {
        for (Path file : files) {
            try {
                sstFileReader.open(file.toString());
                sstFileReader.verifyChecksum();
            } catch (RocksDBException e) {
                throw new IOException(
                        "Error while verifying Checksum of Sst File : " + file.toString() + ". ",
                        e);
            }
        }
    }

    @Override
    public void close() {
        sstFileReader.close();
    }
}
