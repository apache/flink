/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;

import org.apache.commons.io.FileUtils;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

/** Utility class for testing methods for blobs. */
public class TestingBlobUtils {
    private TestingBlobUtils() {
        throw new UnsupportedOperationException(
                String.format("Cannot instantiate %s.", TestingBlobUtils.class.getSimpleName()));
    }

    @Nonnull
    static TransientBlobKey writeTransientBlob(
            Path storageDirectory, JobID jobId, byte[] fileContent) throws IOException {
        return (TransientBlobKey)
                writeBlob(storageDirectory, jobId, fileContent, BlobKey.BlobType.TRANSIENT_BLOB);
    }

    @Nonnull
    static PermanentBlobKey writePermanentBlob(
            Path storageDirectory, JobID jobId, byte[] fileContent) throws IOException {
        return (PermanentBlobKey)
                writeBlob(storageDirectory, jobId, fileContent, BlobKey.BlobType.PERMANENT_BLOB);
    }

    @Nonnull
    static BlobKey writeBlob(
            Path storageDirectory, JobID jobId, byte[] fileContent, BlobKey.BlobType blobType)
            throws IOException {
        final BlobKey blobKey =
                BlobKey.createKey(blobType, BlobUtils.createMessageDigest().digest(fileContent));

        final File storageLocation =
                new File(
                        BlobUtils.getStorageLocationPath(
                                storageDirectory.toString(), jobId, blobKey));
        FileUtils.createParentDirectories(storageLocation);
        FileUtils.writeByteArrayToFile(storageLocation, fileContent);

        return blobKey;
    }
}
