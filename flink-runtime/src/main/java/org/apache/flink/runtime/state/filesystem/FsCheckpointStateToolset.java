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

import org.apache.flink.core.fs.DuplicatingFileSystem;
import org.apache.flink.core.fs.DuplicatingFileSystem.CopyRequest;
import org.apache.flink.core.fs.EntropyInjector;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStateToolset;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * An implementation of {@link CheckpointStateToolset} that does file based duplicating with as
 * {@link DuplicatingFileSystem}.
 */
public class FsCheckpointStateToolset implements CheckpointStateToolset {

    private final Path basePath;
    private final DuplicatingFileSystem fs;

    public FsCheckpointStateToolset(Path basePath, DuplicatingFileSystem fs) {
        this.basePath = basePath;
        this.fs = fs;
    }

    @Override
    public boolean canFastDuplicate(StreamStateHandle stateHandle) throws IOException {
        if (!(stateHandle instanceof FileStateHandle)) {
            return false;
        }
        final Path srcPath = ((FileStateHandle) stateHandle).getFilePath();
        final Path dst = getNewDstPath(srcPath.getName());
        return fs.canFastDuplicate(srcPath, dst);
    }

    @Override
    public List<StreamStateHandle> duplicate(List<StreamStateHandle> stateHandles)
            throws IOException {

        final List<CopyRequest> requests = new ArrayList<>();
        for (StreamStateHandle handle : stateHandles) {
            if (!(handle instanceof FileStateHandle)) {
                throw new IllegalArgumentException("We can duplicate only FileStateHandles.");
            }
            final Path srcPath = ((FileStateHandle) handle).getFilePath();
            requests.add(CopyRequest.of(srcPath, getNewDstPath(srcPath.getName())));
        }
        fs.duplicate(requests);

        return IntStream.range(0, stateHandles.size())
                .mapToObj(
                        idx -> {
                            final StreamStateHandle originalHandle = stateHandles.get(idx);
                            final Path dst = requests.get(idx).getDestination();
                            if (originalHandle instanceof RelativeFileStateHandle) {
                                return new RelativeFileStateHandle(
                                        dst, dst.getName(), originalHandle.getStateSize());
                            } else {
                                return new FileStateHandle(dst, originalHandle.getStateSize());
                            }
                        })
                .collect(Collectors.toList());
    }

    private Path getNewDstPath(String fileName) throws IOException {
        final Path dst = new Path(basePath, fileName);
        return EntropyInjector.addEntropy(dst.getFileSystem(), dst);
    }
}
