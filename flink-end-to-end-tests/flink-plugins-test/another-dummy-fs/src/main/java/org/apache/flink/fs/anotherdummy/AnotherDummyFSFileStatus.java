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

package org.apache.flink.fs.anotherdummy;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

class AnotherDummyFSFileStatus implements FileStatus {
    private final Path path;
    private final int length;

    AnotherDummyFSFileStatus(Path path, int length) {
        this.path = path;
        this.length = length;
    }

    @Override
    public long getLen() {
        return length;
    }

    @Override
    public long getBlockSize() {
        return length;
    }

    @Override
    public short getReplication() {
        return 0;
    }

    @Override
    public long getModificationTime() {
        return 0;
    }

    @Override
    public long getAccessTime() {
        return 0;
    }

    @Override
    public boolean isDir() {
        return false;
    }

    @Override
    public Path getPath() {
        return path;
    }
}
