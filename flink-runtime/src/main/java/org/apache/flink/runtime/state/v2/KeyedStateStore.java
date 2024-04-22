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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;

import static java.util.Objects.requireNonNull;

/** This contains methods for registering keyed state with a managed store. */
public class KeyedStateStore {

    private final AsyncKeyedStateBackend asyncKeyedStateBackend;

    public KeyedStateStore(AsyncKeyedStateBackend asyncKeyedStateBackend) {
        this.asyncKeyedStateBackend = asyncKeyedStateBackend;
    }

    public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            return asyncKeyedStateBackend.getState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }
}
