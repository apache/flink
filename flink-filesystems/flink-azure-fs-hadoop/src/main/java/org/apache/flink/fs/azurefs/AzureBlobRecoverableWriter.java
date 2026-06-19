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

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.runtime.fs.hdfs.HadoopFsRecoverable;
import org.apache.flink.runtime.fs.hdfs.HadoopRecoverableWriter;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/** Recoverable writer for AzureBlob file system. */
public class AzureBlobRecoverableWriter extends HadoopRecoverableWriter {
    /**
     * Creates a new Recoverable writer.
     *
     * @param fs The AzureBlob file system on which the writer operates.
     */
    public AzureBlobRecoverableWriter(FileSystem fs) {
        super(fs);
    }

    protected void checkSupportedFSSchemes(org.apache.hadoop.fs.FileSystem fs) {
        // This writer is only supported on a subset of file systems
        if (!("abfs".equalsIgnoreCase(fs.getScheme())
                || "abfss".equalsIgnoreCase(fs.getScheme()))) {
            throw new UnsupportedOperationException(
                    "Recoverable writers on AzureBlob are only supported for ABFS");
        }
    }

    @Override
    protected RecoverableFsDataOutputStream getRecoverableFsDataOutputStream(
            org.apache.hadoop.fs.Path targetFile, org.apache.hadoop.fs.Path tempFile)
            throws IOException {
        return new AzureBlobFsRecoverableDataOutputStream(fs, targetFile, tempFile);
    }

    @Override
    public RecoverableFsDataOutputStream recover(ResumeRecoverable recoverable) throws IOException {
        return new AzureBlobFsRecoverableDataOutputStream(fs, (HadoopFsRecoverable) recoverable);
    }

    @Override
    public RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable recoverable)
            throws IOException {
        return new AzureBlobFsRecoverableDataOutputStream.ABFSCommitter(
                fs, (HadoopFsRecoverable) recoverable);
    }
}
