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

package org.apache.flink.state.forst;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import java.util.ArrayList;
import java.util.List;

/**
 * The ForSt {@link StateRequestContainer} which can classify the state requests by ForStDB
 * requestType (Get、Put or Iterator).
 */
public class ForStStateRequestClassifier implements StateRequestContainer {

    private final List<ForStDBGetRequest<?, ?, ?, ?>> dbGetRequests;

    private final List<ForStDBPutRequest<?, ?, ?>> dbPutRequests;

    private final List<ForStDBIterRequest<?, ?, ?, ?, ?>> dbIterRequests;

    public ForStStateRequestClassifier() {
        this.dbGetRequests = new ArrayList<>();
        this.dbPutRequests = new ArrayList<>();
        this.dbIterRequests = new ArrayList<>();
    }

    @Override
    public void offer(StateRequest<?, ?, ?, ?> stateRequest) {
        Object forstDbRequest = convertRequests(stateRequest);
        if (forstDbRequest instanceof ForStDBGetRequest) {
            dbGetRequests.add((ForStDBGetRequest<?, ?, ?, ?>) forstDbRequest);
        } else if (forstDbRequest instanceof ForStDBPutRequest) {
            dbPutRequests.add((ForStDBPutRequest<?, ?, ?>) forstDbRequest);
        } else {
            dbIterRequests.add((ForStDBIterRequest<?, ?, ?, ?, ?>) forstDbRequest);
        }
    }

    @Override
    public boolean isEmpty() {
        return dbGetRequests.isEmpty() && dbPutRequests.isEmpty() && dbIterRequests.isEmpty();
    }

    @SuppressWarnings("ConstantConditions")
    public static Object convertRequests(StateRequest<?, ?, ?, ?> stateRequest) {
        StateRequestType stateRequestType = stateRequest.getRequestType();
        switch (stateRequestType) {
            case VALUE_GET:
            case LIST_GET:
            case MAP_GET:
            case MAP_IS_EMPTY:
            case MAP_CONTAINS:
            case REDUCING_GET:
            case AGGREGATING_GET:
                {
                    ForStInnerTable<?, ?, ?> innerTable =
                            (ForStInnerTable<?, ?, ?>) stateRequest.getState();
                    return innerTable.buildDBGetRequest(stateRequest);
                }
            case VALUE_UPDATE:
            case LIST_UPDATE:
            case LIST_ADD:
            case LIST_ADD_ALL:
            case MAP_PUT:
            case MAP_REMOVE:
            case REDUCING_ADD:
            case AGGREGATING_ADD:
                {
                    ForStInnerTable<?, ?, ?> innerTable =
                            (ForStInnerTable<?, ?, ?>) stateRequest.getState();
                    return innerTable.buildDBPutRequest(stateRequest);
                }
            case MAP_ITER:
            case MAP_ITER_KEY:
            case MAP_ITER_VALUE:
            case ITERATOR_LOADING:
                {
                    ForStMapState<?, ?, ?, ?> forStMapState =
                            (ForStMapState<?, ?, ?, ?>) stateRequest.getState();
                    return forStMapState.buildDBIterRequest(stateRequest);
                }
            case MAP_PUT_ALL:
                {
                    ForStMapState<?, ?, ?, ?> forStMapState =
                            (ForStMapState<?, ?, ?, ?>) stateRequest.getState();
                    return forStMapState.buildDBBunchPutRequest(stateRequest);
                }
            case CLEAR:
                {
                    if (stateRequest.getState() instanceof ForStMapState) {
                        ForStMapState<?, ?, ?, ?> forStMapState =
                                (ForStMapState<?, ?, ?, ?>) stateRequest.getState();
                        return forStMapState.buildDBBunchPutRequest(stateRequest);
                    } else if (stateRequest.getState() instanceof ForStInnerTable) {
                        ForStInnerTable<?, ?, ?> innerTable =
                                (ForStInnerTable<?, ?, ?>) stateRequest.getState();
                        return innerTable.buildDBPutRequest(stateRequest);
                    } else {
                        throw new UnsupportedOperationException(
                                "The State "
                                        + stateRequest.getState().getClass()
                                        + " doesn't yet support the clear method.");
                    }
                }
            case CUSTOMIZED:
                {
                    return handleCustomizedStateRequests(stateRequest);
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported state request type:" + stateRequestType);
        }
    }

    @SuppressWarnings("unchecked")
    private static Object handleCustomizedStateRequests(StateRequest<?, ?, ?, ?> stateRequest) {
        Tuple2<ForStStateRequestType, ?> payload =
                (Tuple2<ForStStateRequestType, ?>) stateRequest.getPayload();
        ForStStateRequestType requestType = payload.f0;
        switch (requestType) {
            case LIST_GET_RAW:
                {
                    ForStListState<?, ?, ?> forStListState =
                            (ForStListState<?, ?, ?>) stateRequest.getState();
                    return forStListState.buildDBGetRequest(stateRequest);
                }
            case MERGE_ALL_RAW:
                {
                    ForStListState<?, ?, ?> forStListState =
                            (ForStListState<?, ?, ?>) stateRequest.getState();
                    return forStListState.buildDBPutRequest(stateRequest);
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported customized state request type:" + requestType);
        }
    }

    public List<ForStDBGetRequest<?, ?, ?, ?>> pollDbGetRequests() {
        return dbGetRequests;
    }

    public List<ForStDBPutRequest<?, ?, ?>> pollDbPutRequests() {
        return dbPutRequests;
    }

    public List<ForStDBIterRequest<?, ?, ?, ?, ?>> pollDbIterRequests() {
        return dbIterRequests;
    }

    public long size() {
        return dbGetRequests.size() + dbPutRequests.size() + dbIterRequests.size();
    }
}
