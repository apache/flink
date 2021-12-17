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

package org.apache.flink.core.fs.local;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemBehaviorTestSuite;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/** Behavior tests for Flink's {@link LocalFileSystem}. */
public class LocalFileSystemBehaviorTest extends FileSystemBehaviorTestSuite {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    @Override
    public FileSystem getFileSystem() throws Exception {
        return LocalFileSystem.getSharedInstance();
    }

    @Override
    public Path getBasePath() throws Exception {
        return new Path(tmp.newFolder().toURI());
    }

    @Override
    public FileSystemKind getFileSystemKind() {
        return FileSystemKind.FILE_SYSTEM;
    }
}
