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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

class TestingHeartbeatListenerBuilder<I, O> {
    private Consumer<ResourceID> notifyHeartbeatTimeoutConsumer = ignored -> {};
    private BiConsumer<ResourceID, I> reportPayloadConsumer = (ignoredA, ignoredB) -> {};
    private Function<ResourceID, O> retrievePayloadFunction = ignored -> null;

    public TestingHeartbeatListenerBuilder<I, O> setNotifyHeartbeatTimeoutConsumer(
            Consumer<ResourceID> notifyHeartbeatTimeoutConsumer) {
        this.notifyHeartbeatTimeoutConsumer = notifyHeartbeatTimeoutConsumer;
        return this;
    }

    public TestingHeartbeatListenerBuilder<I, O> setReportPayloadConsumer(
            BiConsumer<ResourceID, I> reportPayloadConsumer) {
        this.reportPayloadConsumer = reportPayloadConsumer;
        return this;
    }

    public TestingHeartbeatListenerBuilder<I, O> setRetrievePayloadFunction(
            Function<ResourceID, O> retrievePayloadFunction) {
        this.retrievePayloadFunction = retrievePayloadFunction;
        return this;
    }

    public TestingHeartbeatListener<I, O> createNewTestingHeartbeatListener() {
        return new TestingHeartbeatListener<>(
                notifyHeartbeatTimeoutConsumer, reportPayloadConsumer, retrievePayloadFunction);
    }
}
