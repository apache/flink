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

package org.apache.flink.runtime.state.filemerging;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.SharedStateRegistryKey;
import org.apache.flink.runtime.state.StreamStateHandle;

import javax.annotation.Nonnull;

import java.nio.file.Path;
import java.util.Optional;

/** Wrap {@link DirectoryStateHandle} to a {@link StreamStateHandle}. */
public class DirectoryStreamStateHandle extends DirectoryStateHandle implements StreamStateHandle {

    private static final long serialVersionUID = 1L;

    public DirectoryStreamStateHandle(@Nonnull Path directory, long directorySize) {
        super(directory, directorySize);
    }

    @Override
    public FSDataInputStream openInputStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<byte[]> asBytesIfInMemory() {
        return Optional.empty();
    }

    @Override
    public PhysicalStateHandleID getStreamStateHandleID() {
        return new PhysicalStateHandleID(getDirectory().toString());
    }

    public SharedStateRegistryKey createStateRegistryKey() {
        return new SharedStateRegistryKey(getDirectory().toUri().toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DirectoryStreamStateHandle that = (DirectoryStreamStateHandle) o;

        return getDirectory().equals(that.getDirectory());
    }

    @Override
    public String toString() {
        return "DirectoryStreamStateHandle{" + "directory=" + getDirectory() + '}';
    }

    /**
     * Return a {@link DirectoryStreamStateHandle} with zero size, which usually used to be
     * registered to {@link org.apache.flink.runtime.state.SharedStateRegistry} to track the life
     * cycle of the directory, therefore a fake size is provided.
     *
     * @param directory the directory.
     * @return DirectoryStreamStateHandle with zero size.
     */
    public static DirectoryStreamStateHandle forPathWithZeroSize(@Nonnull Path directory) {
        return new DirectoryStreamStateHandle(directory, 0);
    }
}
