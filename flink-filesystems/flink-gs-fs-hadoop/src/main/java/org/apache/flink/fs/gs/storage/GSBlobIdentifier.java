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

package org.apache.flink.fs.gs.storage;

import org.apache.flink.util.Preconditions;

import com.google.cloud.storage.BlobId;

import java.util.Objects;

/** An abstraction for the Google BlobId type. */
public class GSBlobIdentifier {

    /** The bucket name. */
    public final String bucketName;

    /** The object name, within the bucket. */
    public final String objectName;

    /**
     * Construct an abstract blob identifier.
     *
     * @param bucketName The bucket name
     * @param objectName The object name
     */
    public GSBlobIdentifier(String bucketName, String objectName) {
        this.bucketName = Preconditions.checkNotNull(bucketName);
        this.objectName = Preconditions.checkNotNull(objectName);
    }

    /**
     * Get a Google blob id for this identifier, with generation=null.
     *
     * @return The BlobId
     */
    public BlobId getBlobId() {
        return BlobId.of(bucketName, objectName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GSBlobIdentifier that = (GSBlobIdentifier) o;
        return bucketName.equals(that.bucketName) && objectName.equals(that.objectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketName, objectName);
    }

    @Override
    public String toString() {
        return "GSBlobIdentifier{"
                + "bucketName='"
                + bucketName
                + '\''
                + ", objectName='"
                + objectName
                + '\''
                + '}';
    }

    /**
     * Construct an abstract blob identifier from a Google BlobId.
     *
     * @param blobId The Google BlobId
     * @return The abstract blob identifier
     */
    public static GSBlobIdentifier fromBlobId(BlobId blobId) {
        return new GSBlobIdentifier(blobId.getBucket(), blobId.getName());
    }
}
