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

import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.util.Preconditions;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;

/** Provides recoverable-writer functionality for the standard GoogleHadoopFileSystem. */
class GSFileSystem extends HadoopFileSystem {

    private final GSFileSystemOptions options;

    GSFileSystem(GoogleHadoopFileSystem googleHadoopFileSystem, GSFileSystemOptions options) {
        super(Preconditions.checkNotNull(googleHadoopFileSystem));
        this.options = Preconditions.checkNotNull(options);
    }

    /* TODO: uncomment with recoverable writer commit
    @Override
    public RecoverableWriter createRecoverableWriter() throws IOException {

        // create the Google storage service instance
        Storage storage = StorageOptions.getDefaultInstance().getService();

        // create the GS blob storage wrapper
        GSBlobStorage blobStorage = new GSBlobStorage(storage);

        // construct the recoverable writer with the blob storage wrapper and the options
        return new GSRecoverableWriter(blobStorage, options);
    }
     */
}
