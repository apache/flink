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

    private final List<ForStDBGetRequest<?, ?>> dbGetRequests;

    private final List<ForStDBPutRequest<?, ?>> dbPutRequests;

    public ForStStateRequestClassifier() {
        this.dbGetRequests = new ArrayList<>();
        this.dbPutRequests = new ArrayList<>();
    }

    @Override
    public void offer(StateRequest<?, ?, ?> stateRequest) {
        convertStateRequestsToForStDBRequests(stateRequest);
    }

    @Override
    public boolean isEmpty() {
        return dbGetRequests.isEmpty() && dbPutRequests.isEmpty();
    }

    @SuppressWarnings("ConstantConditions")
    private void convertStateRequestsToForStDBRequests(StateRequest<?, ?, ?> stateRequest) {
        StateRequestType stateRequestType = stateRequest.getRequestType();
        switch (stateRequestType) {
            case VALUE_GET:
                {
                    ForStValueState<?, ?> forStValueState =
                            (ForStValueState<?, ?>) stateRequest.getState();
                    dbGetRequests.add(forStValueState.buildDBGetRequest(stateRequest));
                    return;
                }
            case VALUE_UPDATE:
                {
                    ForStValueState<?, ?> forStValueState =
                            (ForStValueState<?, ?>) stateRequest.getState();
                    dbPutRequests.add(forStValueState.buildDBPutRequest(stateRequest));
                    return;
                }
            case CLEAR:
                {
                    if (stateRequest.getState() instanceof ForStValueState) {
                        ForStValueState<?, ?> forStValueState =
                                (ForStValueState<?, ?>) stateRequest.getState();
                        dbPutRequests.add(forStValueState.buildDBPutRequest(stateRequest));
                        return;
                    } else {
                        throw new UnsupportedOperationException(
                                "The State "
                                        + stateRequest.getState().getClass()
                                        + " doesn't yet support the clear method.");
                    }
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported state request type:" + stateRequestType);
        }
    }

    public List<ForStDBGetRequest<?, ?>> pollDbGetRequests() {
        return dbGetRequests;
    }

    public List<ForStDBPutRequest<?, ?>> pollDbPutRequests() {
        return dbPutRequests;
    }
}
