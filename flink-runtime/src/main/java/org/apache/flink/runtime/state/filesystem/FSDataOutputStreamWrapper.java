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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/**
 * Implementation of {@link MetadataOutputStreamWrapper} encapsulates the {@link FSDataOutputStream}
 * for {@link FsCheckpointMetadataOutputStream}.
 */
@Internal
public class FSDataOutputStreamWrapper extends MetadataOutputStreamWrapper {
    private final FileSystem fileSystem;
    private final Path metadataFilePath;
    private final FSDataOutputStream out;

    public FSDataOutputStreamWrapper(FileSystem fileSystem, Path metadataFilePath)
            throws IOException {
        this.fileSystem = fileSystem;
        this.metadataFilePath = metadataFilePath;
        this.out = fileSystem.create(metadataFilePath, FileSystem.WriteMode.NO_OVERWRITE);
    }

    @Override
    public FSDataOutputStream getOutput() {
        return out;
    }

    @Override
    public void closeForCommitAction() throws IOException {
        out.close();
    }

    @Override
    public void closeAction() throws IOException {
        out.close();
    }

    @Override
    public void cleanup() throws IOException {
        fileSystem.delete(metadataFilePath, false);
    }
}
