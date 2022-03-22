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

package org.apache.flink.fs.osshadoop;

import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.RefCountedFileWithStream;
import org.apache.flink.core.fs.RefCountedTmpFileCreator;
import org.apache.flink.fs.osshadoop.writer.OSSRecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Implementation of the Flink {@link org.apache.flink.core.fs.FileSystem} interface for Aliyun OSS.
 * This class implements the common behavior implemented directly by Flink and delegates common
 * calls to an implementation of Hadoop's filesystem abstraction.
 */
public class FlinkOSSFileSystem extends HadoopFileSystem {

    // Minimum size of each of or multipart pieces in bytes
    public static final long MULTIPART_UPLOAD_PART_SIZE_MIN = 10L << 20;

    // Size of each of or multipart pieces in bytes
    private long ossUploadPartSize;

    private int maxConcurrentUploadsPerStream;

    private final Executor uploadThreadPool;

    private String localTmpDir;

    private final FunctionWithException<File, RefCountedFileWithStream, IOException>
            cachedFileCreator;

    private OSSAccessor ossAccessor;

    public FlinkOSSFileSystem(
            org.apache.hadoop.fs.FileSystem fileSystem,
            long ossUploadPartSize,
            int maxConcurrentUploadsPerStream,
            String localTmpDirectory,
            OSSAccessor ossAccessor) {
        super(fileSystem);

        Preconditions.checkArgument(ossUploadPartSize >= MULTIPART_UPLOAD_PART_SIZE_MIN);

        this.ossUploadPartSize = ossUploadPartSize;
        this.maxConcurrentUploadsPerStream = maxConcurrentUploadsPerStream;

        this.uploadThreadPool = Executors.newCachedThreadPool();

        // recoverable writer parameter configuration initialization
        // Temporary directory for cache data before uploading to OSS

        this.localTmpDir = Preconditions.checkNotNull(localTmpDirectory);

        this.cachedFileCreator =
                RefCountedTmpFileCreator.inDirectories(new File(localTmpDirectory));

        this.ossAccessor = ossAccessor;
    }

    @Override
    public FileSystemKind getKind() {
        return FileSystemKind.OBJECT_STORE;
    }

    @Override
    public RecoverableWriter createRecoverableWriter() throws IOException {
        return new OSSRecoverableWriter(
                ossAccessor,
                ossUploadPartSize,
                maxConcurrentUploadsPerStream,
                uploadThreadPool,
                cachedFileCreator);
    }

    public String getLocalTmpDir() {
        return localTmpDir;
    }
}
