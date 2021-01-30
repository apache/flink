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

package org.apache.flink.fs.gshadoop;

import org.apache.flink.fs.gshadoop.writer.DefaultGSRecoverableWriterHelper;
import org.apache.flink.fs.gshadoop.writer.GSRecoverableWriterHelper;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.hadoop.fs.FileSystem;

/**
 * The default implementation of the file system helper, which provides access to the real {@link
 * com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem} and {@link
 * com.google.cloud.storage.Storage} implementations.
 */
class DefaultGSFileSystemHelper implements GSFileSystemHelper {

    private final GSRecoverableWriterHelper recoverableWriterHelper;

    /** Construct the file system helper. */
    public DefaultGSFileSystemHelper() {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        this.recoverableWriterHelper = new DefaultGSRecoverableWriterHelper(storage);
    }

    @Override
    public FileSystem createHadoopFileSystem() {
        return new GoogleHadoopFileSystem();
    }

    @Override
    public GSRecoverableWriterHelper getRecoverableWriterHelper() {
        return recoverableWriterHelper;
    }
}
