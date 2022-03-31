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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.StateSnapshot;

import java.io.IOException;

/** Writer for the given state snapshot and key group. */
@Internal
public interface StateSnapshotWriter {
    void write(StateSnapshot snapshot, DataOutputViewStreamWrapper out, int keyGroupId)
            throws IOException;

    StateSnapshotWriter DEFAULT =
            (snapshot, out, keyGroup) ->
                    snapshot.getKeyGroupWriter().writeStateInKeyGroup(out, keyGroup);
}
