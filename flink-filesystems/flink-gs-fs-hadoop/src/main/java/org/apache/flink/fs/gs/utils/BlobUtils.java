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

import org.apache.flink.fs.gs.GSFileSystemFactory;
import org.apache.flink.fs.gs.GSFileSystemOptions;
import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.net.URI;
import java.util.UUID;

/** Utility functions related to blobs. */
public class BlobUtils {

    /** The temporary object prefix. */
    private static final String TEMPORARY_OBJECT_PREFIX = ".inprogress";

    /** The maximum number of blobs that can be composed in a single operation. */
    public static final int COMPOSE_MAX_BLOBS = 32;

    /**
     * Parses a blob id from a Google storage uri, i.e. gs://bucket/foo/bar yields a blob with
     * bucket name "bucket" and object name "foo/bar".
     *
     * @param uri The gs uri
     * @return The blob id
     */
    public static GSBlobIdentifier parseUri(URI uri) {
        Preconditions.checkArgument(
                uri.getScheme().equals(GSFileSystemFactory.SCHEME),
                String.format("URI scheme for %s must be %s", uri, GSFileSystemFactory.SCHEME));

        String finalBucketName = uri.getAuthority();
        if (StringUtils.isNullOrWhitespaceOnly(finalBucketName)) {
            throw new IllegalArgumentException(String.format("Bucket name in %s is invalid", uri));
        }
        String path = uri.getPath();
        if (StringUtils.isNullOrWhitespaceOnly(path)) {
            throw new IllegalArgumentException(String.format("Object name in %s is invalid", uri));
        }
        String finalObjectName = path.substring(1); // remove leading slash from path
        if (StringUtils.isNullOrWhitespaceOnly(finalObjectName)) {
            throw new IllegalArgumentException(String.format("Object name in %s is invalid", uri));
        }
        return new GSBlobIdentifier(finalBucketName, finalObjectName);
    }

    /**
     * Returns the temporary bucket name. If options specifies a temporary bucket name, we use that
     * one; otherwise, we use the bucket name of the final blob.
     *
     * @param finalBlobIdentifier The final blob identifier
     * @param options The file system options
     * @return The temporary bucket name
     */
    public static String getTemporaryBucketName(
            GSBlobIdentifier finalBlobIdentifier, GSFileSystemOptions options) {
        return options.getWriterTemporaryBucketName().orElse(finalBlobIdentifier.bucketName);
    }

    /**
     * Returns a temporary object partial name, i.e. .inprogress/foo/bar/ for the final blob with
     * object name "foo/bar". The included trailing slash is deliberate, so that we can be sure that
     * object names that start with this partial name are, in fact, temporary files associated with
     * the upload of the associated final blob.
     *
     * @param finalBlobIdentifier The final blob identifier
     * @return The temporary object partial name
     */
    public static String getTemporaryObjectPartialName(GSBlobIdentifier finalBlobIdentifier) {
        return String.format(
                "%s/%s/%s/",
                TEMPORARY_OBJECT_PREFIX,
                finalBlobIdentifier.bucketName,
                finalBlobIdentifier.objectName);
    }

    /**
     * Returns a temporary object name, formed by appending the temporary object id to the temporary
     * object partial name, i.e. .inprogress/foo/bar/abc for the final blob with object name
     * "foo/bar" and temporary object id "abc".
     *
     * @param finalBlobIdentifier The final blob identifier
     * @param temporaryObjectId The temporary object id
     * @return The temporary object name
     */
    public static String getTemporaryObjectName(
            GSBlobIdentifier finalBlobIdentifier, UUID temporaryObjectId) {
        return getTemporaryObjectPartialName(finalBlobIdentifier) + temporaryObjectId.toString();
    }

    /**
     * Resolves a temporary blob identifier for a provided temporary object id and the provided
     * options.
     *
     * @param finalBlobIdentifier The final blob identifier
     * @param temporaryObjectId The temporary object id
     * @param options The file system options
     * @return The blob identifier
     */
    public static GSBlobIdentifier getTemporaryBlobIdentifier(
            GSBlobIdentifier finalBlobIdentifier,
            UUID temporaryObjectId,
            GSFileSystemOptions options) {
        String temporaryBucketName = BlobUtils.getTemporaryBucketName(finalBlobIdentifier, options);
        String temporaryObjectName =
                BlobUtils.getTemporaryObjectName(finalBlobIdentifier, temporaryObjectId);
        return new GSBlobIdentifier(temporaryBucketName, temporaryObjectName);
    }
}
