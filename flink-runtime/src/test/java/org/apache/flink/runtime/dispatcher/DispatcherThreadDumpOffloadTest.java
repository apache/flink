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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.rpc.RpcUtils;

import org.junit.After;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that {@link Dispatcher#requestThreadDump} is offloaded to the ioExecutor. */
public class DispatcherThreadDumpOffloadTest extends AbstractDispatcherTest {

    private static final String IO_THREAD_NAME = "test-jm-io-thread";

    private TestingDispatcher dispatcher;
    private ExecutorService ioExecutor;

    @After
    @Override
    public void tearDown() throws Exception {
        if (dispatcher != null) {
            RpcUtils.terminateRpcEndpoint(dispatcher);
        }
        if (ioExecutor != null) {
            ioExecutor.shutdownNow();
        }
        super.tearDown();
    }

    @Test
    public void requestThreadDumpRunsOnIoExecutor() throws Exception {
        // Named single-thread executor: if the dump runs on it, dumpAllThreads() captures
        // the thread and it appears in the returned ThreadDumpInfo.
        ioExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, IO_THREAD_NAME));

        dispatcher = createTestingDispatcherBuilder().setIoExecutor(ioExecutor).build(rpcService);
        dispatcher.start();

        final ThreadDumpInfo dump =
                dispatcher
                        .getSelfGateway(DispatcherGateway.class)
                        .requestThreadDump(TIMEOUT)
                        .get(20, TimeUnit.SECONDS);

        assertThat(dump.getThreadInfos())
                .as("dump must include ioExecutor thread '%s' (proves offload)", IO_THREAD_NAME)
                .anyMatch(t -> IO_THREAD_NAME.equals(t.getThreadName()));
    }
}
