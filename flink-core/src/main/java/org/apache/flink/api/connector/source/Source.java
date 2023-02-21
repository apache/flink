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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * The interface for Source. It acts like a factory class that helps construct the {@link
 * SplitEnumerator} and {@link SourceReader} and corresponding serializers.
 *
 * @param <T> The type of records produced by the source.
 * @param <SplitT> The type of splits handled by the source.
 * @param <EnumChkT> The type of the enumerator checkpoints.
 */
@Public
public interface Source<T, SplitT extends SourceSplit, EnumChkT>
        extends SourceReaderFactory<T, SplitT> {

    /**
     * Get the boundedness of this source.
     *
     * @return the boundedness of this source.
     */
    Boundedness getBoundedness();

    /**
     * Creates a new SplitEnumerator for this source, starting a new input.
     *
     * @param enumContext The {@link SplitEnumeratorContext context} for the split enumerator.
     * @return A new SplitEnumerator.
     * @throws Exception The implementor is free to forward all exceptions directly. Exceptions
     *     thrown from this method cause JobManager failure/recovery.
     */
    SplitEnumerator<SplitT, EnumChkT> createEnumerator(SplitEnumeratorContext<SplitT> enumContext)
            throws Exception;

    /**
     * Restores an enumerator from a checkpoint.
     *
     * @param enumContext The {@link SplitEnumeratorContext context} for the restored split
     *     enumerator.
     * @param checkpoint The checkpoint to restore the SplitEnumerator from.
     * @return A SplitEnumerator restored from the given checkpoint.
     * @throws Exception The implementor is free to forward all exceptions directly. Exceptions
     *     thrown from this method cause JobManager failure/recovery.
     */
    SplitEnumerator<SplitT, EnumChkT> restoreEnumerator(
            SplitEnumeratorContext<SplitT> enumContext, EnumChkT checkpoint) throws Exception;

    // ------------------------------------------------------------------------
    //  serializers for the metadata
    // ------------------------------------------------------------------------

    /**
     * Creates a serializer for the source splits. Splits are serialized when sending them from
     * enumerator to reader, and when checkpointing the reader's current state.
     *
     * @return The serializer for the split type.
     */
    SimpleVersionedSerializer<SplitT> getSplitSerializer();

    /**
     * Creates the serializer for the {@link SplitEnumerator} checkpoint. The serializer is used for
     * the result of the {@link SplitEnumerator#snapshotState(long)} method.
     *
     * @return The serializer for the SplitEnumerator checkpoint.
     */
    SimpleVersionedSerializer<EnumChkT> getEnumeratorCheckpointSerializer();
}
