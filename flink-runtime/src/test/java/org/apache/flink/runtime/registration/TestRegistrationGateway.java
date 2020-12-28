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

package org.apache.flink.runtime.registration;

import org.apache.flink.runtime.rpc.TestingGatewayBase;
import org.apache.flink.util.Preconditions;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/** Mock gateway for {@link RegistrationResponse}. */
public class TestRegistrationGateway extends TestingGatewayBase {

    private final BlockingQueue<RegistrationCall> invocations;

    private final RegistrationResponse[] responses;

    private int pos;

    public TestRegistrationGateway(RegistrationResponse... responses) {
        Preconditions.checkArgument(responses != null && responses.length > 0);

        this.invocations = new LinkedBlockingQueue<>();
        this.responses = responses;
    }

    // ------------------------------------------------------------------------

    public CompletableFuture<RegistrationResponse> registrationCall(UUID leaderId, long timeout) {
        invocations.add(new RegistrationCall(leaderId, timeout));

        RegistrationResponse response = responses[pos];
        if (pos < responses.length - 1) {
            pos++;
        }

        // return a completed future (for a proper value), or one that never completes and will time
        // out (for null)
        return response != null
                ? CompletableFuture.completedFuture(response)
                : futureWithTimeout(timeout);
    }

    public BlockingQueue<RegistrationCall> getInvocations() {
        return invocations;
    }

    // ------------------------------------------------------------------------

    /** Invocation parameters. */
    public static class RegistrationCall {
        private final UUID leaderId;
        private final long timeout;

        public RegistrationCall(UUID leaderId, long timeout) {
            this.leaderId = leaderId;
            this.timeout = timeout;
        }

        public UUID leaderId() {
            return leaderId;
        }

        public long timeout() {
            return timeout;
        }
    }
}
