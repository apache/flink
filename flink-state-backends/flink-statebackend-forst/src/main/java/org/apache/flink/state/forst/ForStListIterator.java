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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.runtime.asyncprocessing.AbstractStateIterator;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import java.util.Collection;

/** The forst implementation for list iterator. */
public class ForStListIterator<V> extends AbstractStateIterator<V> {

    public ForStListIterator(
            State originalState,
            StateRequestType requestType,
            StateRequestHandler stateHandler,
            Collection<V> partialResult) {
        super(originalState, requestType, stateHandler, partialResult);
    }

    @Override
    protected boolean hasNext() {
        return false;
    }

    @Override
    protected Object nextPayloadForContinuousLoading() {
        return null;
    }
}
