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

package org.apache.flink.runtime.state;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Common interface to all incremental {@link KeyedStateHandle}. */
public interface IncrementalKeyedStateHandle
        extends KeyedStateHandle, CheckpointBoundKeyedStateHandle {

    /** Returns the identifier of the state backend from which this handle was created. */
    @Nonnull
    UUID getBackendIdentifier();

    /**
     * Returns a list of all shared states and the corresponding localPath in the backend at the
     * time this was created.
     */
    @Nonnull
    List<HandleAndLocalPath> getSharedStateHandles();

    @Nonnull
    StreamStateHandle getMetaDataStateHandle();

    /** A Holder of StreamStateHandle and the corresponding localPath. */
    final class HandleAndLocalPath implements Serializable {

        private static final long serialVersionUID = 7711754687567545052L;

        StreamStateHandle handle;
        final String localPath;

        public static HandleAndLocalPath of(StreamStateHandle handle, String localPath) {
            checkNotNull(handle, "streamStateHandle cannot be null");
            checkNotNull(localPath, "localPath cannot be null");
            return new HandleAndLocalPath(handle, localPath);
        }

        private HandleAndLocalPath(StreamStateHandle handle, String localPath) {
            this.handle = handle;
            this.localPath = localPath;
        }

        public StreamStateHandle getHandle() {
            return this.handle;
        }

        public String getLocalPath() {
            return this.localPath;
        }

        public long getStateSize() {
            return this.handle.getStateSize();
        }

        /** Replace the StreamStateHandle with the registry returned one. */
        public void replaceHandle(StreamStateHandle registryReturned) {
            checkNotNull(registryReturned);
            this.handle = registryReturned;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof HandleAndLocalPath)) {
                return false;
            }

            HandleAndLocalPath that = (HandleAndLocalPath) o;
            return this.handle.equals(that.handle) && this.localPath.equals(that.localPath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(handle, localPath);
        }
    }
}
