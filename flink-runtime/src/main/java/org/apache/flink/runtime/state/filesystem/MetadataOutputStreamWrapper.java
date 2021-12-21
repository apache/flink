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

import java.io.IOException;

/** The wrapper manages metadata output stream close and commit. */
@Internal
public abstract class MetadataOutputStreamWrapper {
    private volatile boolean closed = false;

    /** Returns {@link FSDataOutputStream} to write and other operations. */
    abstract FSDataOutputStream getOutput();

    /**
     * The abstract function of once closing output stream and committing operation. It will throw
     * {@link IOException} when failed and should be invoked by {@code closeForCommit()} indirectly
     * instead of this function.
     */
    abstract void closeForCommitAction() throws IOException;

    /**
     * The abstract function of once closing output stream operation. It will throw {@link
     * IOException} when failed and should be invoked by {@code close()} indirectly instead of this
     * function.
     */
    abstract void closeAction() throws IOException;

    /**
     * The abstract function of aborting temporary files or doing nothing, which depends on the
     * different output stream implementations. It will throw {@link IOException} when failed.
     */
    abstract void cleanup() throws IOException;

    /**
     * The function will check output stream valid. If it has been closed before, it will throw
     * {@link IOException}. If not, it will invoke {@code closeForCommitAction()} and mark it
     * closed.
     */
    final void closeForCommit() throws IOException {
        if (closed) {
            throw new IOException("The output stream has been closed. This should not happen.");
        }
        closeForCommitAction();
        closed = true;
    }

    /**
     * The function will check output stream valid. If it has been closed before, it will do
     * nothing. If not, it will invoke {@code closeAction()} and mark it closed.
     */
    final void close() throws IOException {
        if (closed) {
            return;
        }
        closeAction();
        closed = true;
    }
}
