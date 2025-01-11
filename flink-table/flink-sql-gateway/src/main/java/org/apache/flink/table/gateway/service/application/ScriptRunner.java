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

package org.apache.flink.table.gateway.service.application;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.util.concurrent.Executors;

import java.io.OutputStream;
import java.util.Collections;
import java.util.UUID;

/** Runner to run the script. It prepares the required dependencies and environment. */
public class ScriptRunner {

    private static final SessionHandle SESSION_HANDLE =
            new SessionHandle(UUID.fromString("013059f8-760f-4390-b74d-d0818bd99365"));

    public static void run(String script) throws Exception {
        run(script, System.out);
    }

    @VisibleForTesting
    public static void run(String script, OutputStream outputStream) throws Exception {
        DefaultContext defaultContext =
                DefaultContext.load(
                        (Configuration)
                                StreamExecutionEnvironment.getExecutionEnvironment(
                                                new Configuration())
                                        .getConfiguration(),
                        Collections.emptyList(),
                        false);
        SessionContext sessionContext =
                SessionContext.create(
                        defaultContext,
                        SESSION_HANDLE,
                        SessionEnvironment.newBuilder()
                                .setSessionEndpointVersion(
                                        SqlGatewayRestAPIVersion.getDefaultVersion())
                                .build(),
                        Executors.newDirectExecutorService());
        try (AutoCloseable ignore = sessionContext::close) {
            new ScriptExecutor(sessionContext, new Printer(outputStream)).execute(script);
        }
    }
}
