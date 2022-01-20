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

package org.apache.flink.runtime.testutils;

import org.apache.flink.runtime.entrypoint.WorkingDirectory;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/** External resource that can create temporary working directories. */
public class WorkingDirectoryResource extends ExternalResource {
    private final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Override
    protected void before() throws Throwable {
        temporaryFolder.create();
    }

    @Override
    protected void after() {
        temporaryFolder.delete();
    }

    public WorkingDirectory createNewWorkingDirectory() throws IOException {
        return WorkingDirectory.create(temporaryFolder.newFolder());
    }
}
