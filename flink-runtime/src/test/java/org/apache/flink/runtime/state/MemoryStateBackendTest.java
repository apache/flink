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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;

/** Tests for the {@link org.apache.flink.runtime.state.memory.MemoryStateBackend}. */
@ExtendWith(ParameterizedTestExtension.class)
public class MemoryStateBackendTest extends StateBackendTestBase<MemoryStateBackend> {

    @Parameters(name = "useAsyncmode")
    public static List<Boolean> modes() {
        return Arrays.asList(true, false);
    }

    @Parameter public boolean useAsyncmode;

    @Override
    protected ConfigurableStateBackend getStateBackend() {
        return new MemoryStateBackend(useAsyncmode);
    }

    @Override
    protected boolean isSerializerPresenceRequiredOnRestore() {
        return true;
    }

    @Override
    protected boolean supportsAsynchronousSnapshots() {
        return useAsyncmode;
    }

    // disable these because the verification does not work for this state backend
    @Override
    @TestTemplate
    void testValueStateRestoreWithWrongSerializers() {}

    @Override
    @TestTemplate
    void testListStateRestoreWithWrongSerializers() {}

    @Override
    @TestTemplate
    void testReducingStateRestoreWithWrongSerializers() {}

    @Override
    @TestTemplate
    void testMapStateRestoreWithWrongSerializers() {}

    @Disabled
    @TestTemplate
    void testConcurrentMapIfQueryable() throws Exception {
        super.testConcurrentMapIfQueryable();
    }
}
