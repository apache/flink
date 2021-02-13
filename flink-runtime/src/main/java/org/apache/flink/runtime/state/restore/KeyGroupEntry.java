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

package org.apache.flink.runtime.state.restore;

import org.apache.flink.annotation.Internal;

/** Part of a savepoint representing data for a single state entry in a key group. */
@Internal
public class KeyGroupEntry {
    private final int kvStateId;
    private final byte[] key;
    private final byte[] value;

    KeyGroupEntry(int kvStateId, byte[] key, byte[] value) {
        this.kvStateId = kvStateId;
        this.key = key;
        this.value = value;
    }

    public int getKvStateId() {
        return kvStateId;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }
}
