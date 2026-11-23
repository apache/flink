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
 * <p>Instances are returned only by {@link ObjectStorageFileSystem#getRichFileStatus(Path)}. {@link
 * FileSystem#listStatus(Path)} continues to return plain {@link FileStatus} objects with no
 * metadata.
 */
@Internal
@Experimental
public interface RichFileStatus extends FileStatus {

    /**
     * Returns user-defined metadata stored in the cloud object.
     *
     * <p>On Azure: blob metadata from {@code PathProperties.getMetadata()}. On S3: object user
     * metadata from {@code HeadObjectResponse.metadata()}. On GCS: custom metadata from {@code
     * BlobInfo.getMetadata()}.
     *
     * @return unmodifiable metadata map; may be empty
     */
    Map<String, String> getMetadata();

    /**
     * Returns the ETag of the object at the time this status was fetched.
     *
     * <p>Used for conditional operations such as {@link ObjectStorageFileSystem#moveVerified(Path,
     * Path, String, String)}.
     *
     * @return ETag string
     */
    String getETag();
}
