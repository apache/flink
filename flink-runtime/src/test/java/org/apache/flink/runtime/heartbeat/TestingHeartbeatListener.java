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

final class TestingHeartbeatListener<I, O> implements HeartbeatListener<I, O> {

    private final Consumer<ResourceID> notifyHeartbeatTimeoutConsumer;

    private final BiConsumer<ResourceID, I> reportPayloadConsumer;

    private final Function<ResourceID, O> retrievePayloadFunction;

    private final Consumer<ResourceID> notifyTargetUnreachableConsumer;

    TestingHeartbeatListener(
            Consumer<ResourceID> notifyHeartbeatTimeoutConsumer,
            BiConsumer<ResourceID, I> reportPayloadConsumer,
            Function<ResourceID, O> retrievePayloadFunction,
            Consumer<ResourceID> notifyTargetUnreachableConsumer) {
        this.notifyHeartbeatTimeoutConsumer = notifyHeartbeatTimeoutConsumer;
        this.reportPayloadConsumer = reportPayloadConsumer;
        this.retrievePayloadFunction = retrievePayloadFunction;
        this.notifyTargetUnreachableConsumer = notifyTargetUnreachableConsumer;
    }

    @Override
    public void notifyHeartbeatTimeout(ResourceID resourceID) {
        notifyHeartbeatTimeoutConsumer.accept(resourceID);
    }

    @Override
    public void notifyTargetUnreachable(ResourceID resourceID) {
        notifyTargetUnreachableConsumer.accept(resourceID);
    }

    @Override
    public void reportPayload(ResourceID resourceID, I payload) {
        reportPayloadConsumer.accept(resourceID, payload);
    }

    @Override
    public O retrievePayload(ResourceID resourceID) {
        return retrievePayloadFunction.apply(resourceID);
    }
}
