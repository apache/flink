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

package org.apache.flink.fs.gs;

import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.gs.storage.GSBlobStorageImpl;
import org.apache.flink.fs.gs.writer.GSRecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.util.Preconditions;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** FileSystem implementation that wraps GoogleHadoopFileSystem and supports RecoverableWriter. */
class GSFileSystem extends HadoopFileSystem {

    private static final Logger LOGGER = LoggerFactory.getLogger(GSFileSystem.class);

    private final GSFileSystemOptions fileSystemOptions;

    private final Storage storage;

    GSFileSystem(
            GoogleHadoopFileSystem googleHadoopFileSystem,
            Storage storage,
            GSFileSystemOptions fileSystemOptions) {
        super(Preconditions.checkNotNull(googleHadoopFileSystem));
        this.fileSystemOptions = Preconditions.checkNotNull(fileSystemOptions);
        this.storage = Preconditions.checkNotNull(storage);
    }

    @Override
    public RecoverableWriter createRecoverableWriter() {
        LOGGER.info("Creating GSRecoverableWriter with file-system options {}", fileSystemOptions);

        // create the GS blob storage wrapper
        GSBlobStorageImpl blobStorage = new GSBlobStorageImpl(storage);

        // construct the recoverable writer with the blob storage wrapper and the options
        return new GSRecoverableWriter(blobStorage, fileSystemOptions);
    }
}
