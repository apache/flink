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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.sequencedmultisetstate.linked.LinkedMultiSetState;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

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
     * Add the given element using a normal (non-multi) set semantics: if a matching element exists
     * already, replace it (the timestamp is updated).
     */
    SizeChangeInfo add(T element, long timestamp) throws Exception;

    /** Add the given element using a multi-set semantics, i.e. append. */
    SizeChangeInfo append(T element, long timestamp) throws Exception;

    /** Get iterator over all remaining elements and their timestamps, in order of insertion. */
    Iterator<Tuple2<T, Long>> iterator() throws Exception;

    /** Tells whether any state exists (in the given key context). */
    boolean isEmpty() throws IOException;

    /**
     * Remove the given element. If there are multiple instances of the same element, remove the
     * first one in insertion order.
     */
    Tuple3<RemovalResultType, Optional<T>, SizeChangeInfo> remove(T element) throws Exception;

    /** Clear the state (in the current key context). */
    void clear();

    /** Load cache. */
    void loadCache() throws IOException;

    /** Clear caches. */
    void clearCache();

    /** Removal Result Type. */
    enum RemovalResultType {
        /**
         * Nothing was removed (e.g. as a result of TTL or not matching key), the result will not
         * contain any elements.
         */
        NOTHING_REMOVED,
        /** All elements were removed. The result will contain the last removed element. */
        ALL_REMOVED,
        /**
         * The most recently added element was removed. The result will contain the element added
         * before it.
         */
        REMOVED_LAST_ADDED,
        /**
         * An element was removed, it was not the most recently added, there are more elements. The
         * result will not contain any elements
         */
        REMOVED_OTHER
    }

    enum Strategy {
        VALUE_STATE,
        MAP_STATE,
        ADAPTIVE
    }

    /**
     * Represents the change in size of a multi-set before and after an operation.
     *
     * <p>This class is used to track the size of the multi-set state before and after a
     * modification, such as adding or removing elements.
     *
     * <p>Fields:
     *
     * <ul>
     *   <li>{@code sizeBefore}: The size of the multi-set before the operation.
     *   <li>{@code sizeAfter}: The size of the multi-set after the operation.
     * </ul>
     *
     * <p>This class is immutable and provides a simple way to encapsulate size change information.
     */
    class SizeChangeInfo {
        public final long sizeBefore;
        public final long sizeAfter;

        public SizeChangeInfo(long sizeBefore, long sizeAfter) {
            this.sizeBefore = sizeBefore;
            this.sizeAfter = sizeAfter;
        }

        public boolean wasEmpty() {
            return sizeBefore == 0;
        }

        public boolean isEmpty() {
            return sizeAfter == 0;
        }

        @Override
        public String toString() {
            return "SizeChangeInfo{"
                    + "sizeBefore="
                    + sizeBefore
                    + ", sizeAfter="
                    + sizeAfter
                    + '}';
        }
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
