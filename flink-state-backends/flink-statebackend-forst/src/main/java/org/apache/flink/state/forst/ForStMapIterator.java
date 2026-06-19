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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.asyncprocessing.AbstractStateIterator;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import org.forstdb.RocksIterator;

import java.util.Collection;

/** The map iterator implementation for ForStDB. */
public class ForStMapIterator<T> extends AbstractStateIterator<T> {

    /**
     * Whether the iterator has encountered the end, which determines the return value of {@link
     * #hasNextLoading}.
     */
    private boolean encounterEnd;

    /**
     * The rocksdb iterator for next loading, should not be null when request type is
     * ITERATOR_LOADING.
     */
    private RocksIterator rocksIterator;

    /** The original result type, mainly used for ITERATOR_LOADING. */
    private StateRequestType originalRequestType;

    public ForStMapIterator(
            State originalState,
            StateRequestType originalRequestType,
            StateRequestType requestType,
            StateRequestHandler stateHandler,
            Collection<T> partialResult,
            boolean encounterEnd,
            RocksIterator rocksIterator) {
        super(originalState, requestType, stateHandler, partialResult);
        this.originalRequestType = originalRequestType;
        this.encounterEnd = encounterEnd;
        this.rocksIterator = rocksIterator;
    }

    @Override
    public boolean hasNextLoading() {
        return !encounterEnd;
    }

    @Override
    protected Object nextPayloadForContinuousLoading() {
        return Tuple2.of(originalRequestType, rocksIterator);
    }
}
