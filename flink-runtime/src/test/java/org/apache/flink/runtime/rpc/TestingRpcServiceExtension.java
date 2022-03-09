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

package org.apache.flink.runtime.rpc;

import org.apache.flink.core.testutils.CustomExtension;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/** Custom extension that starts a {@link TestingRpcService}. */
public class TestingRpcServiceExtension implements CustomExtension {

    @Nullable private TestingRpcService testingRpcService;

    public TestingRpcServiceExtension() {
        this.testingRpcService = null;
    }

    public TestingRpcService getTestingRpcService() {
        Preconditions.checkNotNull(testingRpcService);
        return testingRpcService;
    }

    @Override
    public void before(ExtensionContext extensionContext) {
        if (testingRpcService != null) {
            terminateRpcService(testingRpcService);
        }

        testingRpcService = new TestingRpcService();
    }

    @Override
    public void after(ExtensionContext extensionContext) {
        if (testingRpcService != null) {
            terminateRpcService(testingRpcService);
            testingRpcService = null;
        }
    }

    private void terminateRpcService(TestingRpcService testingRpcService) {
        CompletableFuture<Void> terminationFuture = testingRpcService.stopService();
        terminationFuture.join();
    }
}
