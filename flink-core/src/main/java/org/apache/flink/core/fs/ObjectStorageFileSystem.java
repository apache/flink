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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;

/**
 * Optional capability interface for object-store-native filesystem operations.
 *
 * <p>Checked via {@code instanceof} by callers that need lazy enumeration, per-object metadata
 * fetches, or ETag-guarded atomic rename. Follows the {@link PathsCopyingFileSystem} and {@link
 * EntropyInjectingFileSystem} precedents.
 *
 * <p>Implementations that do not yet support a particular operation may throw {@link
 * UnsupportedOperationException} from that method.
 */
@Internal
@Experimental
public interface ObjectStorageFileSystem {

    /**
     * Returns a lazy iterator of all file paths under the given prefix, recursively.
     *
     * <p>Excludes directory markers. Does not follow symlinks. Does not fetch per-object metadata.
     * Pages are retrieved from the underlying store on demand; no network call is made during this
     * method's execution.
     *
     * <p>Storage failures, including a non-existent prefix, surface as unchecked exceptions from
     * {@code hasNext()} or {@code next()}.
     *
     * @param prefix the root path to enumerate
     * @return lazy iterator of file paths; directory markers excluded; caller must close
     * @throws IOException if the iterator cannot be constructed
     */
    CloseableIterator<Path> listPaths(final Path prefix) throws IOException;

    /**
     * Returns file status with user metadata and ETag populated.
     *
     * <p>Issues one HEAD request per call.
     *
     * @param path the path to fetch status for
     * @return rich status with metadata and ETag
     * @throws FileNotFoundException if the path does not exist
     * @throws UnsupportedOperationException if the implementation does not support this operation
     * @throws IOException on storage error
     */
    RichFileStatus getRichFileStatus(final Path path) throws IOException;

    /**
     * Moves {@code src} to {@code dst}, conditional on both having the expected ETags.
     *
     * <p>On success, {@code src} is deleted. On {@code false} return, both files are untouched.
     *
     * @param src source path
     * @param dst destination path
     * @param srcETag expected ETag of {@code src}
     * @param dstETag expected ETag of {@code dst}; empty string means {@code dst} must not exist
     * @return {@code true} if the move succeeded; {@code false} if any ETag guard fired
     * @throws UnsupportedOperationException if the implementation does not support this operation
     * @throws IOException on storage error; state of {@code src} is undefined
     */
    boolean moveVerified(final Path src, final Path dst, final String srcETag, final String dstETag)
            throws IOException;
}
