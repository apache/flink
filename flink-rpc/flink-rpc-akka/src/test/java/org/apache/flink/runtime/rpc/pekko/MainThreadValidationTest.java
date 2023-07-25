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

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that the {@link PekkoRpcService} runs all RPCs in the {@link PekkoRpcActor}'s main thread.
 */
class MainThreadValidationTest {

    @Test
    void failIfNotInMainThread() throws Exception {
        // test if assertions are activated. The test only works if assertions are loaded.
        try {
            assert false;
            // apparently they are not activated
            return;
        } catch (AssertionError ignored) {
        }

        // actual test
        PekkoRpcService pekkoRpcService =
                new PekkoRpcService(
                        PekkoUtils.createDefaultActorSystem(),
                        PekkoRpcServiceConfiguration.defaultConfiguration());

        try {
            TestEndpoint testEndpoint = new TestEndpoint(pekkoRpcService);
            testEndpoint.start();

            // this works, because it is executed as an RPC call
            testEndpoint.getSelfGateway(TestGateway.class).someConcurrencyCriticalFunction();

            // this fails, because it is executed directly
            assertThatThrownBy(() -> testEndpoint.someConcurrencyCriticalFunction())
                    .isInstanceOf(AssertionError.class);

            testEndpoint.closeAsync();
        } finally {
            pekkoRpcService.closeAsync().get();
        }
    }

    // ------------------------------------------------------------------------
    //  test RPC endpoint
    // ------------------------------------------------------------------------

    interface TestGateway extends RpcGateway {

        void someConcurrencyCriticalFunction();
    }

    private static class TestEndpoint extends RpcEndpoint implements TestGateway {

        private TestEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public void someConcurrencyCriticalFunction() {
            validateRunsInMainThread();
        }
    }
}
