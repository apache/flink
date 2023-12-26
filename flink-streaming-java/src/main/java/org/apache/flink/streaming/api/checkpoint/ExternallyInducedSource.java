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

package org.apache.flink.streaming.api.checkpoint;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.FlinkException;

/**
 * Sources that implement this interface delay checkpoints when receiving a trigger message from the
 * checkpoint coordinator to the point when their input data/events indicate that a checkpoint
 * should be triggered.
 *
 * <p>Since sources cannot simply create a new checkpoint on their own, this mechanism always goes
 * together with a {@link WithMasterCheckpointHook hook on the master side}. In a typical setup, the
 * hook on the master tells the source system (for example the message queue) to prepare a
 * checkpoint. The exact point when the checkpoint is taken is then controlled by the event stream
 * received from the source, and triggered by the source function (implementing this interface) in
 * Flink when seeing the relevant events.
 *
 * @param <T> Type of the elements produced by the source function
 * @param <CD> The type of the data stored in the checkpoint by the master that triggers
 * @deprecated This interface is based on the {@link
 *     org.apache.flink.streaming.api.functions.source.SourceFunction} API, which is due to be
 *     removed. Use the new {@link org.apache.flink.api.connector.source.Source} API instead.
 */
@Deprecated
@PublicEvolving
public interface ExternallyInducedSource<T, CD>
        extends SourceFunction<T>, WithMasterCheckpointHook<CD> {

    /**
     * Sets the checkpoint trigger through which the source can trigger the checkpoint.
     *
     * @param checkpointTrigger The checkpoint trigger to set
     */
    void setCheckpointTrigger(CheckpointTrigger checkpointTrigger);

    // ------------------------------------------------------------------------

    /**
     * Through the {@code CheckpointTrigger}, the source function notifies the Flink source operator
     * when to trigger the checkpoint.
     */
    interface CheckpointTrigger {

        /**
         * Triggers a checkpoint. This method should be called by the source when it sees the event
         * that indicates that a checkpoint should be triggered.
         *
         * <p>When this method is called, the parallel operator instance in which the calling source
         * function runs will perform its checkpoint and insert the checkpoint barrier into the data
         * stream.
         *
         * @param checkpointId The ID that identifies the checkpoint.
         * @throws FlinkException Thrown when the checkpoint could not be triggered, for example
         *     because of an invalid state or errors when storing the checkpoint state.
         */
        void triggerCheckpoint(long checkpointId) throws FlinkException;
    }
}
