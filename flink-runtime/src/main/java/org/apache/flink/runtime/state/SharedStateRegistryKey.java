/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.StringBasedID;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * This class represents a key that uniquely identifies (on a logical level) state handles for
 * registration in the {@link SharedStateRegistry}. Two files which should logically be the same
 * should have the same {@link SharedStateRegistryKey}. The meaning of logical equivalence is up to
 * the application.
 */
public class SharedStateRegistryKey extends StringBasedID {

    private static final long serialVersionUID = 1L;

    public SharedStateRegistryKey(String prefix, StateHandleID stateHandleID) {
        super(prefix + '-' + stateHandleID);
    }

    @VisibleForTesting
    public SharedStateRegistryKey(String keyString) {
        super(keyString);
    }

    /** Create a unique key based on physical id. */
    public static SharedStateRegistryKey forStreamStateHandle(StreamStateHandle handle) {
        String keyString = handle.getStreamStateHandleID().getKeyString();
        // key strings tend to be longer, so we use the MD5 of the key string to save memory
        return new SharedStateRegistryKey(
                UUID.nameUUIDFromBytes(keyString.getBytes(StandardCharsets.UTF_8)).toString());
    }
}
