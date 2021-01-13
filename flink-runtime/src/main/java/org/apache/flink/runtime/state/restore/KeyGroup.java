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

/** Part of a savepoint representing data for a single key group. */
@Internal
public class KeyGroup {
    private final int keyGroupId;
    private final ThrowingIterator<KeyGroupEntry> keyGroupEntries;

    KeyGroup(int keyGroupId, ThrowingIterator<KeyGroupEntry> keyGroupEntries) {
        this.keyGroupId = keyGroupId;
        this.keyGroupEntries = keyGroupEntries;
    }

    public int getKeyGroupId() {
        return keyGroupId;
    }

    public ThrowingIterator<KeyGroupEntry> getKeyGroupEntries() {
        return keyGroupEntries;
    }
}
