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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import java.util.Collection;
import java.util.Collections;

/**
 * Tests for the keyed state backend and operator state backend, as created by the {@link
 * FsStateBackend}.
 */
public class FileStateBackendMigrationTest extends StateBackendMigrationTestBase<FsStateBackend> {

    @Parameters
    public static Collection<Object> modes() {
        // current test does not need to be parameterized
        return Collections.singletonList("no-use");
    }

    @Override
    protected FsStateBackend getStateBackend() throws Exception {
        return new FsStateBackend(tempFolder.toURI(), false);
    }
}
