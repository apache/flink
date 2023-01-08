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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.slots.ResourceRequirement;

import java.util.Collection;
import java.util.function.BiConsumer;

/** Builder for the {@link TestingResourceEventListener}. */
public class TestingResourceEventListenerBuilder {
    private BiConsumer<JobID, Collection<ResourceRequirement>> notEnoughResourceAvailableConsumer =
            (ignoredA, ignoredB) -> {};

    public TestingResourceEventListenerBuilder setNotEnoughResourceAvailableConsumer(
            BiConsumer<JobID, Collection<ResourceRequirement>> notEnoughResourceAvailableConsumer) {
        this.notEnoughResourceAvailableConsumer = notEnoughResourceAvailableConsumer;
        return this;
    }

    public TestingResourceEventListener build() {
        return new TestingResourceEventListener(notEnoughResourceAvailableConsumer);
    }
}
