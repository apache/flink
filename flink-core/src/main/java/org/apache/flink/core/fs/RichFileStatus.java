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

import java.util.Map;

/**
 * A {@link FileStatus} enriched with cloud-object user metadata and an ETag.
 *
 * <p>Returned by {@link ObjectStorageFileSystem#getRichFileStatus(Path)} and used in
 * re-encryption workflows to read encryption headers and obtain a stable ETag before a conditional
 * move.
 *
 * @see ObjectStorageFileSystem
 */
@Internal
@Experimental
public interface RichFileStatus extends FileStatus {

    /**
     * Returns user-defined blob metadata (e.g., Azure {@code PathProperties.getMetadata()}).
     *
     * @return an unmodifiable map of metadata key-value pairs; never {@code null}
     */
    Map<String, String> getMetadata();

    /**
     * Returns the ETag of the object at fetch time.
     *
     * <p>Used for {@link ObjectStorageFileSystem#moveVerified} to guard against concurrent
     * modifications.
     *
     * @return the ETag string; never {@code null}
     */
    String getETag();
}
