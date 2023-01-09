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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/** Base class for ABFS and Hadoop recoverable stream. */
@Internal
public abstract class BaseHadoopFsRecoverableFsDataOutputStream
        extends RecoverableFsDataOutputStream {

    protected FileSystem fs;

    protected Path targetFile;

    protected Path tempFile;

    protected FSDataOutputStream out;

    // In ABFS outputstream we need to add this to the current pos
    protected long initialFileSize = 0;

    public long getPos() throws IOException {
        return out.getPos();
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        out.hsync();
    }

    @Override
    public void sync() throws IOException {
        out.hflush();
        out.hsync();
    }

    @Override
    public RecoverableWriter.ResumeRecoverable persist() throws IOException {
        sync();
        return createHadoopFsRecoverable(getPos());
    }

    public HadoopFsRecoverable createHadoopFsRecoverable(long pos) throws IOException {
        return new HadoopFsRecoverable(targetFile, tempFile, pos + initialFileSize);
    }

    @Override
    public abstract Committer closeForCommit() throws IOException;

    @Override
    public void close() throws IOException {
        out.close();
    }
}
