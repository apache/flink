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

package org.apache.flink.fs.gs.utils;

import com.google.cloud.storage.BlobId;

import java.net.URI;

/** Utility functions related to blobs. */
public class BlobUtils {

    /** The maximum number of blobs that can be composed in a single operation. */
    public static final int COMPOSE_MAX_BLOBS = 32;

    /**
     * Normalizes a blob id, ensuring that the generation is null.
     *
     * @param blobId The blob id
     * @return The blob id with the generation set to null
     */
    public static BlobId normalizeBlobId(BlobId blobId) {
        return BlobId.of(blobId.getBucket(), blobId.getName());
    }

    /**
     * Parses a blob id from a Google storage uri, i.e. gs://bucket/foo/bar yields a blob with
     * bucket name "bucket" and object name "foo/bar".
     *
     * @param uri The gs uri
     * @return The blob id
     */
    public static BlobId parseUri(URI uri) {
        String finalBucketName = uri.getAuthority();
        if (finalBucketName == null) {
            throw new IllegalArgumentException(String.format("Bucket name in %s is invalid", uri));
        }
        String path = uri.getPath();
        if (path == null) {
            throw new IllegalArgumentException(String.format("Object name in %s is invalid", uri));
        }
        String finalObjectName = path.substring(1); // remove leading slash from path
        if (finalObjectName.isEmpty()) {
            throw new IllegalArgumentException(String.format("Object name in %s is invalid", uri));
        }
        return BlobId.of(finalBucketName, finalObjectName);
    }
}
