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

package org.apache.flink.fs.s3.common;

import org.apache.flink.core.fs.EntropyInjectingFileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.s3.common.utils.RefCountedFileWithStream;
import org.apache.flink.fs.s3.common.utils.RefCountedTmpFileCreator;
import org.apache.flink.fs.s3.common.writer.S3AccessHelper;
import org.apache.flink.fs.s3.common.writer.S3RecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Implementation of the Flink {@link org.apache.flink.core.fs.FileSystem} interface for S3. This
 * class implements the common behavior implemented directly by Flink and delegates common calls to
 * an implementation of Hadoop's filesystem abstraction.
 */
public class FlinkS3FileSystem extends HadoopFileSystem implements EntropyInjectingFileSystem {

    @Nullable private final String entropyInjectionKey;

    private final int entropyLength;

    // ------------------- Recoverable Writer Parameters -------------------

    /** The minimum size of a part in the multipart upload, except for the last part: 5 MIBytes. */
    public static final long S3_MULTIPART_MIN_PART_SIZE = 5L << 20;

    private final String localTmpDir;

    private final FunctionWithException<File, RefCountedFileWithStream, IOException> tmpFileCreator;

    @Nullable private final S3AccessHelper s3AccessHelper;

    private final Executor uploadThreadPool;

    private final long s3uploadPartSize;

    private final int maxConcurrentUploadsPerStream;

    /**
     * Creates a FlinkS3FileSystem based on the given Hadoop S3 file system. The given Hadoop file
     * system object is expected to be initialized already.
     *
     * <p>This constructor additionally configures the entropy injection for the file system.
     *
     * @param hadoopS3FileSystem The Hadoop FileSystem that will be used under the hood.
     * @param entropyInjectionKey The substring that will be replaced by entropy or removed.
     * @param entropyLength The number of random alphanumeric characters to inject as entropy.
     */
    public FlinkS3FileSystem(
            org.apache.hadoop.fs.FileSystem hadoopS3FileSystem,
            String localTmpDirectory,
            @Nullable String entropyInjectionKey,
            int entropyLength,
            @Nullable S3AccessHelper s3UploadHelper,
            long s3uploadPartSize,
            int maxConcurrentUploadsPerStream) {

        super(hadoopS3FileSystem);

        if (entropyInjectionKey != null && entropyLength <= 0) {
            throw new IllegalArgumentException(
                    "Entropy length must be >= 0 when entropy injection key is set");
        }

        this.entropyInjectionKey = entropyInjectionKey;
        this.entropyLength = entropyLength;

        // recoverable writer parameter configuration initialization
        this.localTmpDir = Preconditions.checkNotNull(localTmpDirectory);
        this.tmpFileCreator = RefCountedTmpFileCreator.inDirectories(new File(localTmpDirectory));
        this.s3AccessHelper = s3UploadHelper;
        this.uploadThreadPool = Executors.newCachedThreadPool();

        Preconditions.checkArgument(s3uploadPartSize >= S3_MULTIPART_MIN_PART_SIZE);
        this.s3uploadPartSize = s3uploadPartSize;
        this.maxConcurrentUploadsPerStream = maxConcurrentUploadsPerStream;
    }

    // ------------------------------------------------------------------------

    @Nullable
    @Override
    public String getEntropyInjectionKey() {
        return entropyInjectionKey;
    }

    @Override
    public String generateEntropy() {
        return StringUtils.generateRandomAlphanumericString(
                ThreadLocalRandom.current(), entropyLength);
    }

    @Override
    public FileSystemKind getKind() {
        return FileSystemKind.OBJECT_STORE;
    }

    public String getLocalTmpDir() {
        return localTmpDir;
    }

    @Override
    public RecoverableWriter createRecoverableWriter() throws IOException {
        if (s3AccessHelper == null) {
            // this is the case for Presto
            throw new UnsupportedOperationException(
                    "This s3 file system implementation does not support recoverable writers.");
        }

        return S3RecoverableWriter.writer(
                getHadoopFileSystem(),
                tmpFileCreator,
                s3AccessHelper,
                uploadThreadPool,
                s3uploadPartSize,
                maxConcurrentUploadsPerStream);
    }
}
