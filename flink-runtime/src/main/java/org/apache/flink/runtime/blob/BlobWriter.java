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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.types.Either;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/** BlobWriter is used to upload data to the BLOB store. */
public interface BlobWriter {

    Logger LOG = LoggerFactory.getLogger(BlobWriter.class);

    /**
     * Uploads the data of the given byte array for the given job to the BLOB server and makes it a
     * permanent BLOB.
     *
     * @param jobId the ID of the job the BLOB belongs to
     * @param value the buffer to upload
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while writing it to a local file, or
     *     uploading it to the HA store
     */
    PermanentBlobKey putPermanent(JobID jobId, byte[] value) throws IOException;

    /**
     * Uploads the data from the given input stream for the given job to the BLOB server and makes
     * it a permanent BLOB.
     *
     * @param jobId ID of the job this blob belongs to
     * @param inputStream the input stream to read the data from
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while reading the data from the input
     *     stream, writing it to a local file, or uploading it to the HA store
     */
    PermanentBlobKey putPermanent(JobID jobId, InputStream inputStream) throws IOException;

    /**
     * Delete the uploaded data with the given {@link JobID} and {@link PermanentBlobKey}.
     *
     * @param jobId ID of the job this blob belongs to
     * @param permanentBlobKey the key of this blob
     */
    boolean deletePermanent(JobID jobId, PermanentBlobKey permanentBlobKey);

    /**
     * Returns the min size before data will be offloaded to the BLOB store.
     *
     * @return minimum offloading size
     */
    int getMinOffloadingSize();

    /**
     * Serializes the given value and offloads it to the BlobServer if its size exceeds the minimum
     * offloading size of the BlobServer.
     *
     * @param value to serialize
     * @param jobId to which the value belongs.
     * @param blobWriter to use to offload the serialized value
     * @param <T> type of the value to serialize
     * @return Either the serialized value or the stored blob key
     * @throws IOException if the data cannot be serialized
     */
    static <T> Either<SerializedValue<T>, PermanentBlobKey> serializeAndTryOffload(
            T value, JobID jobId, BlobWriter blobWriter) throws IOException {
        Preconditions.checkNotNull(value);

        final SerializedValue<T> serializedValue = new SerializedValue<>(value);

        return tryOffload(serializedValue, jobId, blobWriter);
    }

    static <T> Either<SerializedValue<T>, PermanentBlobKey> tryOffload(
            SerializedValue<T> serializedValue, JobID jobId, BlobWriter blobWriter) {
        Preconditions.checkNotNull(serializedValue);
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(blobWriter);

        if (serializedValue.getByteArray().length < blobWriter.getMinOffloadingSize()) {
            return Either.Left(serializedValue);
        } else {
            try {
                final PermanentBlobKey permanentBlobKey =
                        blobWriter.putPermanent(jobId, serializedValue.getByteArray());

                return Either.Right(permanentBlobKey);
            } catch (IOException e) {
                LOG.warn("Failed to offload value for job {} to BLOB store.", jobId, e);

                return Either.Left(serializedValue);
            }
        }
    }
}
