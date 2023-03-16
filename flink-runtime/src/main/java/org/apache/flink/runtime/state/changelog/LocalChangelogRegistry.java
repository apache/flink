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

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.Closeable;
import java.io.IOException;

/** This registry is responsible for deleting changlog's local handles which are not in use. */
@Internal
public interface LocalChangelogRegistry extends Closeable {
    LocalChangelogRegistry NO_OP =
            new LocalChangelogRegistry() {

                @Override
                public void register(StreamStateHandle handle, long checkpointID) {}

                @Override
                public void discardUpToCheckpoint(long upTo) {}

                @Override
                public void close() throws IOException {}
            };

    /**
     * Called upon ChangelogKeyedStateBackend#notifyCheckpointComplete.
     *
     * @param handle handle to register.
     * @param checkpointID latest used checkpointID.
     */
    void register(StreamStateHandle handle, long checkpointID);

    /**
     * Called upon ChangelogKeyedStateBackend#notifyCheckpointComplete and
     * ChangelogKeyedStateBackend#notifyCheckpointSubsumed. Remote dstl handles are unregistered
     * when {@link CompletedCheckpointStore#addCheckpointAndSubsumeOldestOne}, local dtsl handles
     * are unregistered when the checkpoint completes, because only one checkpoint is kept for local
     * recovery.
     *
     * @param upTo lowest CheckpointID which is still valid.
     */
    void discardUpToCheckpoint(long upTo);
}
