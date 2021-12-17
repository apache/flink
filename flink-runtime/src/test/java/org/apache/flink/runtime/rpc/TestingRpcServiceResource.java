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

import org.junit.rules.ExternalResource;

import java.util.concurrent.CompletableFuture;

/**
 * {@link ExternalResource} which starts a {@link org.apache.flink.runtime.rpc.TestingRpcService}.
 */
public class TestingRpcServiceResource extends ExternalResource {

    private TestingRpcService testingRpcService;

    public TestingRpcServiceResource() {
        this.testingRpcService = null;
    }

    public TestingRpcService getTestingRpcService() {
        checkInitialized();
        return testingRpcService;
    }

    private void checkInitialized() {
        assert (testingRpcService != null);
    }

    @Override
    protected void before() {
        if (testingRpcService != null) {
            terminateRpcService();
        }

        testingRpcService = new TestingRpcService();
    }

    @Override
    protected void after() {
        if (testingRpcService != null) {
            terminateRpcService();
            testingRpcService = null;
        }
    }

    private void terminateRpcService() {
        CompletableFuture<Void> terminationFuture = testingRpcService.stopService();
        terminationFuture.join();
    }
}
