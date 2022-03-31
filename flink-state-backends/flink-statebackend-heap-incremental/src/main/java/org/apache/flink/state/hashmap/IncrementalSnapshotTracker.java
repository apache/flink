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

package org.apache.flink.state.hashmap;

import org.apache.flink.annotation.Internal;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

/**
 * Tracks snapshots for {@link IncrementalHeapSnapshotStrategy}. Thread-safe: {@link #track(long,
 * IncrementalSnapshot, List)} called from the uploader.
 */
@Internal
@ThreadSafe // todo: use mailbox to call track() instead of synchronization
interface IncrementalSnapshotTracker {

    /** Get base for the next snapshot. */
    IncrementalSnapshot getCurrentBase();

    void track(
            long checkpointId,
            IncrementalSnapshot incrementalSnapshot,
            List<Runnable> confirmCallbacks);

    /** The confirmed snapshot will become the new base (if it is the latest one). */
    void confirmSnapshot(long checkpointId);
}
