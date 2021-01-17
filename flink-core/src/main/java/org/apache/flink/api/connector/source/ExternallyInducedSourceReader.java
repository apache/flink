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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;

import java.util.Optional;

/**
 * Sources that implement this interface do not trigger checkpoints when receiving a trigger message
 * from the checkpoint coordinator, but when their input data/events indicate that a checkpoint
 * should be triggered.
 *
 * <p>The ExternallyInducedSourceReader tells the Flink runtime that a checkpoint needs to be made
 * by returning a checkpointId when shouldTriggerCheckpoint() is invoked.
 *
 * <p>The implementations typically works together with the SplitEnumerator which informs the
 * external system to trigger a checkpoint. The external system also needs to forward the Checkpoint
 * ID to the source, so the source knows which checkpoint to trigger.
 *
 * <p><b>Important:</b> It is crucial that all parallel source tasks trigger their checkpoints at
 * roughly the same time. Otherwise this leads to performance issues due to long checkpoint
 * alignment phases or large alignment data snapshots.
 *
 * @param <T> The type of records produced by the source.
 * @param <SplitT> The type of splits handled by the source.
 */
@Experimental
@PublicEvolving
public interface ExternallyInducedSourceReader<T, SplitT extends SourceSplit>
        extends SourceReader<T, SplitT> {

    /**
     * A method that informs the Flink runtime whether a checkpoint should be triggered on this
     * Source.
     *
     * <p>This method is invoked when the previous {@link #pollNext(ReaderOutput)} returns {@link
     * org.apache.flink.core.io.InputStatus#NOTHING_AVAILABLE}, to check if the source needs to be
     * checkpointed.
     *
     * <p>If a CheckpointId is returned, a checkpoint will be triggered on this source reader.
     * Otherwise, Flink runtime will continue to process the records.
     *
     * @return An optional checkpoint ID that Flink runtime should take a checkpoint for.
     */
    Optional<Long> shouldTriggerCheckpoint();
}
