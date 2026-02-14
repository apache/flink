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

package org.apache.flink.fs.s3native;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

@Internal
public class S3FileStatus implements FileStatus {

    private final long length;
    private final long blockSize;
    private final long modificationTime;
    private final long accessTime;
    private final boolean isDir;
    private final Path path;

    public S3FileStatus(
            long length,
            long blockSize,
            long modificationTime,
            long accessTime,
            boolean isDir,
            Path path) {
        this.length = length;
        this.blockSize = blockSize;
        this.modificationTime = modificationTime;
        this.accessTime = accessTime;
        this.isDir = isDir;
        this.path = path;
    }

    @Override
    public long getLen() {
        return length;
    }

    @Override
    public long getBlockSize() {
        return blockSize;
    }

    @Override
    public short getReplication() {
        return 1;
    }

    @Override
    public long getModificationTime() {
        return modificationTime;
    }

    @Override
    public long getAccessTime() {
        return accessTime;
    }

    @Override
    public boolean isDir() {
        return isDir;
    }

    @Override
    public Path getPath() {
        return path;
    }
}
