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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

/**
 * Internal state interface for the stateBackend layer, where state requests are executed
 * synchronously. Unlike the user-facing state interface, {@code InternalSyncState}'s interfaces
 * provide the key directly in the method signature, so there is no need to pass the keyContext
 * information for these interfaces.
 *
 * @param <K> Type of the key in the state.
 */
@Internal
public interface InternalSyncState<K> {

    /**
     * Removes the value mapped under the given key.
     *
     * @param key The key to be deleted.
     * @throws IOException Thrown when the performed I/O operation fails.
     */
    void clear(K key) throws IOException;

    /**
     * Indicates whether MultiGet API is supported.
     *
     * @return True if supported, false if not.
     */
    default boolean isSupportMultiGet() {
        return false;
    }
}
