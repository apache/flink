/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.hashmap;

import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;

import org.junit.Ignore;

/** {@link IncrementalHashMapStateBackend} test. */
public class IncrementalHeapStateBackendTest
        extends StateBackendTestBase<IncrementalHashMapStateBackend> {
    @Override
    protected ConfigurableStateBackend getStateBackend() {
        return new IncrementalHashMapStateBackend();
    }

    @Override
    protected boolean isSerializerPresenceRequiredOnRestore() {
        return false; // todo: remove this method?
    }

    @Override
    protected boolean supportsAsynchronousSnapshots() {
        return true;
    }

    @Override
    protected CheckpointStorage getCheckpointStorage() {
        return new JobManagerCheckpointStorage();
    }

    @Ignore("Updating metadata (e.g. serializers) is not supported, see FLINK-26853")
    @Override
    public void testKryoRegisteringRestoreResilienceWithRegisteredSerializer() {
        //        super.testKryoRegisteringRestoreResilienceWithRegisteredSerializer();
    }

    @Ignore("Updating metadata (e.g. serializers) is not supported, see FLINK-26853")
    @Override
    public void testKryoRegisteringRestoreResilienceWithDefaultSerializer() {
        //        super.testKryoRegisteringRestoreResilienceWithDefaultSerializer();
    }
}
