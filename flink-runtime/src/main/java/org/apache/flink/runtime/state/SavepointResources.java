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

import org.apache.flink.annotation.Internal;

/**
 * Savepoint resources for a {@link KeyedStateBackend}. This is only a container for the {@link
 * FullSnapshotResources} that will be used by the {@link SavepointSnapshotStrategy} and gives the
 * backend a way to tell the {@link SnapshotStrategyRunner} whether it prefers asynchronous or
 * synchronous writing.
 *
 * @param <K> type of the backend keys.
 */
@Internal
public class SavepointResources<K> {

    private final FullSnapshotResources<K> snapshotResources;
    private final SnapshotExecutionType preferredSnapshotExecutionType;

    public SavepointResources(
            FullSnapshotResources<K> snapshotResources,
            SnapshotExecutionType preferredSnapshotExecutionType) {
        this.snapshotResources = snapshotResources;
        this.preferredSnapshotExecutionType = preferredSnapshotExecutionType;
    }

    public FullSnapshotResources<K> getSnapshotResources() {
        return snapshotResources;
    }

    public SnapshotExecutionType getPreferredSnapshotExecutionType() {
        return preferredSnapshotExecutionType;
    }
}
