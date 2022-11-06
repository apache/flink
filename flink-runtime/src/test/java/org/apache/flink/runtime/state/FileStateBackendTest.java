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

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for the keyed state backend and operator state backend, as created by the {@link
 * FsStateBackend}.
 */
public class FileStateBackendTest extends StateBackendTestBase<FsStateBackend> {

    @Parameters(name = "useAsyncMode={0}")
    public static List<Boolean> modes() {
        return Arrays.asList(true, false);
    }

    @Override
    protected ConfigurableStateBackend getStateBackend() throws Exception {
        return new FsStateBackend(checkpointPath.toURI(), useAsyncMode);
    }

    @Parameter public boolean useAsyncMode;

    @TempDir public static File checkpointPath;

    @Override
    protected boolean isSerializerPresenceRequiredOnRestore() {
        return true;
    }

    @Override
    protected boolean supportsAsynchronousSnapshots() {
        return useAsyncMode;
    }

    // disable these because the verification does not work for this state backend
    @Override
    @TestTemplate
    public void testValueStateRestoreWithWrongSerializers() {}

    @Override
    @TestTemplate
    public void testListStateRestoreWithWrongSerializers() {}

    @Override
    @TestTemplate
    public void testReducingStateRestoreWithWrongSerializers() {}

    @Override
    @TestTemplate
    public void testMapStateRestoreWithWrongSerializers() {}

    @Disabled
    @TestTemplate
    public void testConcurrentMapIfQueryable() throws Exception {
        super.testConcurrentMapIfQueryable();
    }
}
