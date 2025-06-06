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

import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnull;

import java.util.Objects;

/** Base class for all registered state in state backends. */
public abstract class RegisteredStateMetaInfoBase {

    /** The name of the state */
    @Nonnull protected final String name;

    public RegisteredStateMetaInfoBase(@Nonnull String name) {
        this.name = name;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public abstract StateMetaInfoSnapshot snapshot();

    /**
     * create a new metadata object with Lazy serializer provider using existing one as a snapshot.
     * Sometimes metadata was just created or updated, but its StateSerializerProvider will not
     * allow further updates. So this method could replace it with a new one that contains a fresh
     * LazilyRegisteredStateSerializerProvider.
     */
    @Nonnull
    public abstract RegisteredStateMetaInfoBase withSerializerUpgradesAllowed();

    public static RegisteredStateMetaInfoBase fromMetaInfoSnapshot(
            @Nonnull StateMetaInfoSnapshot snapshot) {

        final StateMetaInfoSnapshot.BackendStateType backendStateType =
                snapshot.getBackendStateType();
        switch (backendStateType) {
            case KEY_VALUE:
                return new RegisteredKeyValueStateBackendMetaInfo<>(snapshot);
            case OPERATOR:
                return new RegisteredOperatorStateBackendMetaInfo<>(snapshot);
            case BROADCAST:
                return new RegisteredBroadcastStateBackendMetaInfo<>(snapshot);
            case PRIORITY_QUEUE:
                return new RegisteredPriorityQueueStateBackendMetaInfo<>(snapshot);
            case KEY_VALUE_V2:
                if (snapshot.getOption(
                                StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString())
                        .equals(StateDescriptor.Type.MAP.toString())) {
                    return new org.apache.flink.runtime.state.v2
                            .RegisteredKeyAndUserKeyValueStateBackendMetaInfo<>(snapshot);
                } else {
                    return new org.apache.flink.runtime.state.v2
                            .RegisteredKeyValueStateBackendMetaInfo<>(snapshot);
                }
            default:
                throw new IllegalArgumentException(
                        "Unknown backend state type: " + backendStateType);
        }
    }

    /** Returns a wrapper that can be used as a key in {@link java.util.Map}. */
    public final Key asMapKey() {
        return new Key(this);
    }

    /**
     * Wrapper class that can be used to represent the wrapped {@link RegisteredStateMetaInfoBase}
     * as key in a {@link java.util.Map}.
     */
    public static final class Key {
        private final RegisteredStateMetaInfoBase registeredStateMetaInfoBase;

        private Key(RegisteredStateMetaInfoBase metaInfoBase) {
            this.registeredStateMetaInfoBase = metaInfoBase;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key that = (Key) o;
            return Objects.equals(
                            registeredStateMetaInfoBase.getName(),
                            that.registeredStateMetaInfoBase.getName())
                    && Objects.equals(
                            registeredStateMetaInfoBase.getClass(),
                            that.registeredStateMetaInfoBase.getClass());
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    registeredStateMetaInfoBase.getName(), registeredStateMetaInfoBase.getClass());
        }

        public RegisteredStateMetaInfoBase getRegisteredStateMetaInfoBase() {
            return registeredStateMetaInfoBase;
        }
    }
}
