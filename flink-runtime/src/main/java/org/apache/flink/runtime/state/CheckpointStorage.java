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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;

import java.io.IOException;

/**
 * CheckpointStorage defines how {@link StateBackend}'s store their state for fault tolerance in
 * streaming applications. Various implementations store their checkpoints in different fashions and
 * have different requirements and availability guarantees.
 *
 * <p>For example, {@link org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage
 * JobManagerCheckpointStorage} stores checkpoints in the memory of the JobManager. It is
 * lightweight and without additional dependencies but is not scalable and only supports small state
 * sizes. This checkpoint storage policy is convenient for local testing and development.
 *
 * <p>{@link org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage
 * FileSystemCheckpointStorage} stores checkpoints in a filesystem. For systems like HDFS, NFS
 * Drives, S3, and GCS, this storage policy supports large state size, in the magnitude of many
 * terabytes while providing a highly available foundation for stateful applications. This
 * checkpoint storage policy is recommended for most production deployments.
 *
 * <h2>Raw Bytes Storage</h2>
 *
 * <p>The {@code CheckpointStorage} creates services for <i>raw bytes storage</i>.
 *
 * <p>The <i>raw bytes storage</i> (through the {@link CheckpointStreamFactory}) is the fundamental
 * service that simply stores bytes in a fault tolerant fashion. This service is used by the
 * JobManager to store checkpoint and recovery metadata and is typically also used by the keyed- and
 * operator state backends to store checkpointed state.
 *
 * <h2>Serializability</h2>
 *
 * <p>Implementations need to be {@link java.io.Serializable serializable}, because they distributed
 * across parallel processes (for distributed execution) together with the streaming application
 * code.
 *
 * <p>Because of that, {@code CheckpointStorage} implementations are meant to be like
 * <i>factories</i> that create the proper states stores that provide access to the persistent. That
 * way, the Checkpoint Storage can be very lightweight (contain only configurations) which makes it
 * easier to be serializable.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>Checkpoint storage implementations have to be thread-safe. Multiple threads may be creating
 * streams concurrently.
 */
@PublicEvolving
public interface CheckpointStorage extends java.io.Serializable {

    /**
     * Resolves the given pointer to a checkpoint/savepoint into a checkpoint location. The location
     * supports reading the checkpoint metadata, or disposing the checkpoint storage location.
     *
     * @param externalPointer The external checkpoint pointer to resolve.
     * @return The checkpoint location handle.
     * @throws IOException Thrown, if the state backend does not understand the pointer, or if the
     *     pointer could not be resolved due to an I/O error.
     */
    CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException;

    /**
     * Creates a storage for checkpoints for the given job. The checkpoint storage is used to write
     * checkpoint data and metadata.
     *
     * @param jobId The job to store checkpoint data for.
     * @return A checkpoint storage for the given job.
     * @throws IOException Thrown if the checkpoint storage cannot be initialized.
     */
    CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException;
}
