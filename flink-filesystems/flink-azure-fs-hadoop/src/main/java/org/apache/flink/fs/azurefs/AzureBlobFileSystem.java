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

package org.apache.flink.fs.azurefs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/** Wraps the hadoop's file system to create AzureBlobFileSystem. */
@Internal
class AzureBlobFileSystem extends HadoopFileSystem {
    /**
     * Wraps the given Hadoop File System object as a Flink File System object. The given Hadoop
     * file system object is expected to be initialized already.
     *
     * @param hadoopFileSystem The Azure Blob FileSystem that will be used under the hood.
     */
    AzureBlobFileSystem(FileSystem hadoopFileSystem) {
        super(hadoopFileSystem);
    }

    @Override
    public RecoverableWriter createRecoverableWriter() throws IOException {
        return new AzureBlobRecoverableWriter(getHadoopFileSystem());
    }
}
