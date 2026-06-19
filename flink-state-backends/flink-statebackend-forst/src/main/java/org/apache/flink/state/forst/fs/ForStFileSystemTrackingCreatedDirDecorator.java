/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.fs;

import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A decorator of {@link ForStFlinkFileSystem} to adapt ForSt to the underlying FileSystems which
 * are implemented with incomplete mkdir(), i.e., such FileSystem implementation does not actually
 * create the directory when mkdir() completes. This can lead to unexpected behavior when ForSt
 * tries to assert the existence of directories by calling exists(). Therefore, we track the paths
 * of the should-be-created directories and subsequently return true for existence checks.
 */
public class ForStFileSystemTrackingCreatedDirDecorator extends ForStFlinkFileSystem {
    private final Set<Path> createdDirPaths = new HashSet<>();

    ForStFileSystemTrackingCreatedDirDecorator(ForStFlinkFileSystem fileSystem) {
        super(fileSystem);
    }

    @Override
    public synchronized boolean mkdirs(Path path) throws IOException {
        boolean mkdirSucceed = super.mkdirs(path);
        if (!mkdirSucceed) {
            return false;
        }

        createdDirPaths.add(path);
        return true;
    }

    @Override
    public synchronized boolean exists(final Path f) throws IOException {
        if (createdDirPaths.contains(f)) {
            return true;
        }
        return super.exists(f);
    }
}
