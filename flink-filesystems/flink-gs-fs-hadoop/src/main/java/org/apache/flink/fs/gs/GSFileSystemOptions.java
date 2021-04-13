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

import org.apache.flink.util.Preconditions;

/** The GS file system options. */
public class GSFileSystemOptions {

    /**
     * The default value for the temporary bucket, i.e. empty string which means use same bucket as
     * the final blob being written
     */
    public static final String DEFAULT_WRITER_TEMPORARY_BUCKET_NAME = "";

    /** The default value for the temporary object prefix. */
    public static final String DEFAULT_WRITER_TEMPORARY_OBJECT_PREFIX = ".inprogress/";

    /** The default value for the content type of blobs written by the recoverable writer. */
    public static final String DEFAULT_WRITER_CONTENT_TYPE = "application/octet-stream";

    /** The default value for the writer chunk size, i.e. zero which means use Google default * */
    public static final int DEFAULT_WRITER_CHUNK_SIZE = 0;

    /**
     * The temporary bucket name to use for recoverable writes. If empty, use the same bucket as the
     * final blob to write.
     */
    public final String writerTemporaryBucketName;

    /** The prefix to be applied to the final object name when generating temporary object names. */
    public final String writerTemporaryObjectPrefix;

    /** The content type used for files written by the recoverable writer. */
    public final String writerContentType;

    /**
     * The chunk size to use for writes on the underlying Google WriteChannel. If zero, then the
     * chunk size is not set on the underlying channel, and the default value is used.
     */
    public final int writerChunkSize;

    /**
     * Constructs an options instance.
     *
     * @param writerTemporaryBucketName The temporary bucket name, if empty use same bucket as final
     *     blob
     * @param writerTemporaryObjectPrefix The temporary object prefix
     * @param writerContentType The content type
     * @param writerChunkSize The chunk size, if zero this means use Google default
     */
    public GSFileSystemOptions(
            String writerTemporaryBucketName,
            String writerTemporaryObjectPrefix,
            String writerContentType,
            int writerChunkSize) {
        this.writerTemporaryBucketName = Preconditions.checkNotNull(writerTemporaryBucketName);
        this.writerTemporaryObjectPrefix = Preconditions.checkNotNull(writerTemporaryObjectPrefix);
        this.writerContentType = Preconditions.checkNotNull(writerContentType);
        Preconditions.checkArgument(writerChunkSize >= 0);
        this.writerChunkSize = writerChunkSize;
    }
}
