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

import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.heap.StateSnapshotWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class IncrementalStateSnapshotWriter implements StateSnapshotWriter {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalStateSnapshotWriter.class);
    private final IncrementalSnapshot snapshotBase;

    public IncrementalStateSnapshotWriter(IncrementalSnapshot snapshotBase) {
        this.snapshotBase = snapshotBase;
    }

    @Override
    public void write(StateSnapshot snapshot, DataOutputViewStreamWrapper out, int keyGroupId)
            throws IOException {
        if (snapshot instanceof IncrementalCopyOnWriteStateTableSnapshot) {
            writeIncremental(
                    (IncrementalCopyOnWriteStateTableSnapshot<?, ?, ?>) snapshot, keyGroupId, out);
        } else {
            LOG.trace("write full map snapshot in key group: {}", keyGroupId);
            snapshot.getKeyGroupWriter().writeStateInKeyGroup(out, keyGroupId);
        }
    }

    private void writeIncremental(
            IncrementalCopyOnWriteStateTableSnapshot<?, ?, ?> snapshot,
            int keyGroupId,
            DataOutputViewStreamWrapper out)
            throws IOException {
        LOG.trace("write incremental map snapshot in key group: {}", keyGroupId);
        snapshot.writeStateInKeyGroup(
                out, keyGroupId, snapshotBase.getVersions(snapshot.getStateName(), keyGroupId));
    }
}
