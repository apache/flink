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

package org.apache.flink.table.gateway.rest.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Utility for setting up a rest client based on {@link RestClient} with default settings. */
public class TestingRestClient extends RestClient {

    private final ExecutorService executorService;

    private TestingRestClient(ExecutorService executorService) throws ConfigurationException {
        super(new Configuration(), executorService);
        this.executorService = executorService;
    }

    public static TestingRestClient getTestingRestClient() throws Exception {
        return new TestingRestClient(
                Executors.newFixedThreadPool(
                        1, new ExecutorThreadFactory("rest-client-thread-pool")));
    }

    public void shutdown() throws Exception {
        ExecutorUtils.gracefulShutdown(1, TimeUnit.SECONDS, executorService);
        super.closeAsync().get();
    }
}
