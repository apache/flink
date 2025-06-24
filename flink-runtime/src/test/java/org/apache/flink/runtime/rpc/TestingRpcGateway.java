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

package org.apache.flink.runtime.rpc;

import java.util.function.Supplier;

/** {@code TestingRpcGateway} is a generic test implementation of {@link RpcGateway}. */
public class TestingRpcGateway implements RpcGateway {

    private final Supplier<String> addressSupplier;
    private final Supplier<String> hostnameSupplier;

    private TestingRpcGateway(Supplier<String> addressSupplier, Supplier<String> hostnameSupplier) {
        this.addressSupplier = addressSupplier;
        this.hostnameSupplier = hostnameSupplier;
    }

    @Override
    public String getAddress() {
        return addressSupplier.get();
    }

    @Override
    public String getHostname() {
        return hostnameSupplier.get();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** {@code Builder} for {@code TestingRpcGateway}. */
    public static class Builder {

        private Supplier<String> addressSupplier = () -> "address";
        private Supplier<String> hostnameSupplier = () -> "hostname";

        private Builder() {}

        public Builder setAddressSupplier(Supplier<String> addressSupplier) {
            this.addressSupplier = addressSupplier;
            return this;
        }

        public Builder setHostnameSupplier(Supplier<String> hostnameSupplier) {
            this.hostnameSupplier = hostnameSupplier;
            return this;
        }

        public TestingRpcGateway build() {
            return new TestingRpcGateway(addressSupplier, hostnameSupplier);
        }
    }
}
