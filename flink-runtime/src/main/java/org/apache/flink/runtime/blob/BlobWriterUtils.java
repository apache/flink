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

import java.io.IOException;
import java.util.Optional;

public class BlobWriterUtils {
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
    public static <T> Either<SerializedValue<T>, PermanentBlobKey> serializeAndTryOffload(
            T value, JobID jobId, BlobWriter blobWriter) throws IOException {
        Preconditions.checkNotNull(value);

        final SerializedValue<T> serializedValue = new SerializedValue<>(value);

        return tryOffload(serializedValue, jobId, blobWriter);
    }

    private static <T> Either<SerializedValue<T>, PermanentBlobKey> tryOffload(
            SerializedValue<T> serializedValue, JobID jobId, BlobWriter blobWriter) {
        Preconditions.checkNotNull(serializedValue);
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(blobWriter);

        if (serializedValue.getByteArray().length < blobWriter.getMinOffloadingSize()) {
            return Either.Left(serializedValue);
        } else {
            return offloadWithException(serializedValue, jobId, blobWriter)
                    .map(Either::<SerializedValue<T>, PermanentBlobKey>Right)
                    .orElse(Either.Left(serializedValue));
        }
    }

    /**
     * Offloads the given value to the BlobServer.
     *
     * @param serializedValue to offload
     * @param jobId to which the value belongs.
     * @param blobWriter to use to offload the serialized value
     * @param <T> type of the value to serialize
     * @return the stored blob key
     */
    public static <T> Optional<PermanentBlobKey> offloadWithException(
            SerializedValue<T> serializedValue, JobID jobId, BlobWriter blobWriter) {
        Preconditions.checkNotNull(serializedValue);
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(blobWriter);
        try {
            final PermanentBlobKey permanentBlobKey =
                    blobWriter.putPermanent(jobId, serializedValue.getByteArray());
            return Optional.of(permanentBlobKey);
        } catch (IOException e) {
            BlobWriter.LOG.warn("Failed to offload value for job {} to BLOB store.", jobId, e);
            return Optional.empty();
        }
    }

    private BlobWriterUtils() {}
}
