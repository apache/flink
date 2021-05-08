/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.registration;

import org.apache.flink.util.FlinkException;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/** Default {@link TestRegistrationGateway} implementation. */
public class DefaultTestRegistrationGateway implements TestRegistrationGateway {
    private final String address;
    private final String hostname;
    private final BiFunction<UUID, Long, CompletableFuture<RegistrationResponse>>
            registrationFunction;

    private DefaultTestRegistrationGateway(
            String address,
            String hostname,
            BiFunction<UUID, Long, CompletableFuture<RegistrationResponse>> registrationFunction) {
        this.address = address;
        this.hostname = hostname;
        this.registrationFunction = registrationFunction;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public String getHostname() {
        return hostname;
    }

    @Override
    public CompletableFuture<RegistrationResponse> registrationCall(UUID leaderId, long timeout) {
        return registrationFunction.apply(leaderId, timeout);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for the {@link DefaultTestRegistrationGateway}. */
    public static final class Builder {
        private String address = "localhost";
        private String hostname = "localhost";
        private BiFunction<UUID, Long, CompletableFuture<RegistrationResponse>>
                registrationFunction =
                        (ignoredA, ignoredB) ->
                                CompletableFuture.completedFuture(
                                        new RegistrationResponse.Failure(
                                                new FlinkException("Not configured")));

        public Builder setAddress(String address) {
            this.address = address;
            return this;
        }

        public Builder setHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder setRegistrationFunction(
                BiFunction<UUID, Long, CompletableFuture<RegistrationResponse>>
                        registrationFunction) {
            this.registrationFunction = registrationFunction;
            return this;
        }

        public DefaultTestRegistrationGateway build() {
            return new DefaultTestRegistrationGateway(address, hostname, registrationFunction);
        }
    }
}
