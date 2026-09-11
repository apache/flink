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
 * Optional capability for object storage filesystems that support rich metadata access and
 * ETag-guarded atomic renames.
 *
 * <p>Implementations expose per-object metadata (e.g., encryption headers) and a conditional
 * rename primitive used by re-encryption workflows. Discovered via {@code instanceof}.
 *
 * @see RichFileStatus
 */
@Internal
@Experimental
public interface ObjectStorageFileSystem {

    /**
     * Lazily enumerates all file paths under {@code prefix}, recursively.
     *
     * <p>The returned iterator must be closed to release any paging cursors or SDK connections.
     *
     * @param prefix the path prefix to enumerate; must not be {@code null}
     * @return a closeable, lazy iterator over all file paths under {@code prefix}
     * @throws IOException if enumeration cannot be started
     */
    CloseableIterator<Path> pathsList(Path prefix) throws IOException;

    /**
     * Fetches file status with user metadata and ETag populated.
     *
     * <p>Performs one HEAD-equivalent request per call.
     *
     * @param path the path to query; must not be {@code null}
     * @return file status enriched with user metadata and ETag
     * @throws IOException if the status cannot be fetched
     */
    RichFileStatus getRichFileStatus(Path path) throws IOException;

    /**
     * ETag-guarded atomic rename: moves {@code src} to {@code dst} only if the ETags match.
     *
     * <p>The move succeeds only when the source ETag matches {@code srcETag} and, if {@code
     * dstETag} is non-empty, the destination ETag matches {@code dstETag}. An empty {@code dstETag}
     * asserts that the destination does not exist.
     *
     * @param src source path; must not be {@code null}
     * @param dst destination path; must not be {@code null}
     * @param srcETag expected ETag of the source object; must not be {@code null}
     * @param dstETag expected ETag of the destination object, or {@code ""} to assert non-existence
     * @return {@code true} if the move succeeded; {@code false} if any ETag guard fired
     * @throws IOException if the storage operation itself failed and the source state is unknown
     */
    boolean moveVerified(Path src, Path dst, String srcETag, String dstETag) throws IOException;
}
