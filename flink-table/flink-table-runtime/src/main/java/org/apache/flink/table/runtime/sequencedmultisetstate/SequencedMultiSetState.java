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

package org.apache.flink.table.runtime.sequencedmultisetstate;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.sequencedmultisetstate.linked.LinkedMultiSetState;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class represents an interface for managing an ordered multi-set state in Apache Flink. It
 * provides methods to add, append, and remove elements while maintaining insertion order.
 *
 * <p>The state supports two types of semantics for adding elements:
 *
 * <ul>
 *   <li><b>Normal Set Semantics:</b> Replaces an existing matching element with the new one.
 *   <li><b>Multi-Set Semantics:</b> Appends the new element, allowing duplicates.
 * </ul>
 *
 * <p>Removal operations are supported with different result types, indicating the outcome of the
 * removal process, such as whether all elements were removed, the last added element was removed,
 * or no elements were removed.
 *
 * @param <T> The type of elements stored in the state.
 */
@Internal
public interface SequencedMultiSetState<T> {

    /**
     * Add the given element using the normal (non-multi) set semantics: if a matching element
     * exists already, replace it (the timestamp is updated).
     */
    StateChangeInfo<T> add(T element, long timestamp) throws Exception;

    /** Add the given element using the multi-set semantics, i.e. append. */
    StateChangeInfo<T> append(T element, long timestamp) throws Exception;

    /**
     * Remove the given element. If there are multiple instances of the same element, remove the
     * first one in insertion order.
     */
    StateChangeInfo<T> remove(T element) throws Exception;

    /** Represents a result of a state changing operation. */
    class StateChangeInfo<T> {
        private final StateChangeType changeType;
        private final long sizeBefore;
        private final long sizeAfter;
        @Nullable private final T payload; // depends on the change type

        public static <T> StateChangeInfo<T> forAddition(long sizeBefore, long sizeAfter) {
            return new StateChangeInfo<>(sizeBefore, sizeAfter, StateChangeType.ADDITION, null);
        }

        public static <T> StateChangeInfo<T> forRemovedLastAdded(
                long sizeBefore, long sizeAfter, T payload) {
            return new StateChangeInfo<>(
                    sizeBefore, sizeAfter, StateChangeType.REMOVAL_LAST_ADDED, payload);
        }

        public static <T> StateChangeInfo<T> forRemovedOther(long sizeBefore, long sizeAfter) {
            return new StateChangeInfo<>(
                    sizeBefore, sizeAfter, StateChangeType.REMOVAL_OTHER, null);
        }

        public static <T> StateChangeInfo<T> forAllRemoved(
                long sizeBefore, long sizeAfter, T payload) {
            return new StateChangeInfo<>(
                    sizeBefore, sizeAfter, StateChangeType.REMOVAL_ALL, payload);
        }

        public static <T> StateChangeInfo<T> forRemovalNotFound(long size) {
            return new StateChangeInfo<>(size, size, StateChangeType.REMOVAL_NOT_FOUND, null);
        }

        private StateChangeInfo(
                long sizeBefore, long sizeAfter, StateChangeType changeType, @Nullable T payload) {
            changeType.validate(sizeBefore, sizeAfter, payload);
            this.sizeBefore = sizeBefore;
            this.sizeAfter = sizeAfter;
            this.changeType = changeType;
            this.payload = payload;
        }

        public long getSizeAfter() {
            return sizeAfter;
        }

        public boolean wasEmpty() {
            return sizeBefore == 0;
        }

        public StateChangeType getChangeType() {
            return changeType;
        }

        /** The payload depends on the {@link StateChangeType}. */
        public Optional<T> getPayload() {
            return Optional.ofNullable(payload);
        }
    }

    /** Get iterator over all remaining elements and their timestamps, in order of insertion. */
    Iterator<Tuple2<T, Long>> iterator() throws Exception;

    /** Tells whether any state exists (in the given key context). */
    boolean isEmpty() throws IOException;

    /** Clear the state (in the current key context). */
    void clear();

    /** Load cache. */
    void loadCache() throws IOException;

    /** Clear caches. */
    void clearCache();

    /** Removal Result Type. */
    enum StateChangeType {
        /**
         * An element was added or appended to the state. The result will not contain any elements.
         */
        ADDITION {
            @Override
            public <T> void validate(long sizeBefore, long sizeAfter, T payload) {
                checkArgument(sizeAfter == sizeBefore + 1 || sizeAfter == sizeBefore);
            }
        },
        /**
         * Nothing was removed (e.g. as a result of TTL or not matching key), the result will not
         * contain any elements.
         */
        REMOVAL_NOT_FOUND {
            @Override
            public <T> void validate(long sizeBefore, long sizeAfter, T payload) {
                checkArgument(sizeAfter == sizeBefore);
                checkArgument(payload == null);
            }
        },
        /** All elements were removed. The result will contain the last removed element. */
        REMOVAL_ALL {
            @Override
            public <T> void validate(long sizeBefore, long sizeAfter, T payload) {
                checkArgument(sizeBefore > 0);
                checkArgument(sizeAfter == 0);
                checkNotNull(payload);
            }
        },
        /**
         * The most recently added element was removed. The result will contain the element added
         * before it.
         */
        REMOVAL_LAST_ADDED {
            @Override
            public <T> void validate(long sizeBefore, long sizeAfter, T payload) {
                checkArgument(sizeAfter == sizeBefore - 1);
                checkNotNull(payload);
            }
        },
        /**
         * An element was removed, it was not the most recently added, there are more elements. The
         * result will not contain any elements
         */
        REMOVAL_OTHER {
            @Override
            public <T> void validate(long sizeBefore, long sizeAfter, T payload) {
                checkArgument(sizeAfter == sizeBefore - 1);
                checkArgument(payload == null);
            }
        };

        public abstract <T> void validate(long sizeBefore, long sizeAfter, T payload);
    }

    enum Strategy {
        VALUE_STATE,
        MAP_STATE,
        ADAPTIVE
    }

    static SequencedMultiSetState<RowData> create(
            SequencedMultiSetStateContext parameters,
            RuntimeContext ctx,
            String backendTypeIdentifier) {
        switch (parameters.config.getStrategy()) {
            case MAP_STATE:
                return LinkedMultiSetState.create(parameters, ctx);
            case VALUE_STATE:
                return ValueStateMultiSetState.create(parameters, ctx);
            case ADAPTIVE:
                return AdaptiveSequencedMultiSetState.create(
                        parameters.config,
                        backendTypeIdentifier,
                        ValueStateMultiSetState.create(parameters, ctx),
                        LinkedMultiSetState.create(parameters, ctx));
            default:
                throw new UnsupportedOperationException(parameters.config.getStrategy().name());
        }
    }
}
