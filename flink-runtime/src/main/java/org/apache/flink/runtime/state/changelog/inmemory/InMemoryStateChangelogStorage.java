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

package org.apache.flink.runtime.state.changelog.inmemory;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleReader;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.util.CloseableIterator;

/** An in-memory (non-production) implementation of {@link StateChangelogStorage}. */
public class InMemoryStateChangelogStorage
        implements StateChangelogStorage<InMemoryChangelogStateHandle> {

    @Override
    public InMemoryStateChangelogWriter createWriter(
            String operatorID, KeyGroupRange keyGroupRange) {
        return new InMemoryStateChangelogWriter(keyGroupRange);
    }

    @Override
    public StateChangelogHandleReader<InMemoryChangelogStateHandle> createReader() {
        return handle -> CloseableIterator.fromList(handle.getChanges(), change -> {});
    }
}
