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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.jobgraph.OperatorID;

import java.util.concurrent.CompletableFuture;

/** A simple testing implementation of the {@link CoordinationRequestHandler}. */
public class TestingCoordinationRequestHandler extends TestingOperatorCoordinator
        implements CoordinationRequestHandler {

    public TestingCoordinationRequestHandler(Context context) {
        super(context);
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        Request req = (Request) request;
        return CompletableFuture.completedFuture(new Response<>(req.getPayload()));
    }

    /**
     * A testing stub for an {@link OperatorCoordinator.Provider} that creates a {@link
     * TestingCoordinationRequestHandler}.
     */
    public static final class Provider implements OperatorCoordinator.Provider {

        private static final long serialVersionUID = 1L;

        private final OperatorID operatorId;

        public Provider(OperatorID operatorId) {
            this.operatorId = operatorId;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            return new TestingCoordinationRequestHandler(context);
        }
    }

    /**
     * A {@link CoordinationRequest} that a {@link TestingCoordinationRequestHandler} receives.
     *
     * @param <T> payload type
     */
    public static class Request<T> implements CoordinationRequest {

        private static final long serialVersionUID = 1L;

        private final T payload;

        public Request(T payload) {
            this.payload = payload;
        }

        public T getPayload() {
            return payload;
        }
    }

    /**
     * A {@link CoordinationResponse} that a {@link TestingCoordinationRequestHandler} gives.
     *
     * @param <T> payload type
     */
    public static class Response<T> implements CoordinationResponse {

        private static final long serialVersionUID = 1L;

        private final T payload;

        public Response(T payload) {
            this.payload = payload;
        }

        public T getPayload() {
            return payload;
        }
    }
}
