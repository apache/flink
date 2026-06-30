/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

@Internal
public interface RecoveryCheckpointTrigger {

    /**
     * Atomically snapshots the undrained spill slice and inserts matching {@link
     * RecoveryCheckpointBarrier}s into in-recovery channels. Returns an independent reader over the
     * remaining segments; the caller owns and must close it.
     */
    FetchedChannelStateReader snapshotAndInsertBarriers(long checkpointId) throws IOException;

    /** Returns an empty reader (no spill files, so no segments) and inserts no barriers. */
    RecoveryCheckpointTrigger NO_OP = checkpointId -> FetchedChannelStateReader.emptyReader();

    RecoveryCheckpointTrigger NOT_READY =
            ign -> {
                throw new IllegalStateException("RecoveryCheckpointTrigger is not ready yet");
            };
}
