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
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/** Change of state of a keyed operator. Used for generic incremental checkpoints. */
@Internal
public class StateChange implements Serializable {

    /* For metadata, see FLINK-23035.*/
    public static final int META_KEY_GROUP = -1;

    private static final long serialVersionUID = 1L;

    private final int keyGroup;
    private final byte[] change;

    StateChange(byte[] change) {
        this.keyGroup = META_KEY_GROUP;
        this.change = Preconditions.checkNotNull(change);
    }

    StateChange(int keyGroup, byte[] change) {
        Preconditions.checkArgument(keyGroup >= 0);
        this.keyGroup = keyGroup;
        this.change = Preconditions.checkNotNull(change);
    }

    public static StateChange ofMetadataChange(byte[] change) {
        return new StateChange(change);
    }

    public static StateChange ofDataChange(int keyGroup, byte[] change) {
        return new StateChange(keyGroup, change);
    }

    @Override
    public String toString() {
        return String.format("keyGroup=%d, dataSize=%d", keyGroup, change.length);
    }

    public int getKeyGroup() {
        return keyGroup;
    }

    public byte[] getChange() {
        return change;
    }
}
