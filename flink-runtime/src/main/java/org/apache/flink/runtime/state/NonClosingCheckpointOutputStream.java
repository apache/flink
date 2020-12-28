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

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.ResourceGuard.Lease;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Abstract class to implement custom checkpoint output streams which should not be closable for
 * user code.
 *
 * @param <T> type of the returned state handle.
 */
public abstract class NonClosingCheckpointOutputStream<T extends StreamStateHandle>
        extends OutputStream {

    protected final CheckpointStreamFactory.CheckpointStateOutputStream delegate;
    private final ResourceGuard resourceGuard = new ResourceGuard();

    public NonClosingCheckpointOutputStream(
            CheckpointStreamFactory.CheckpointStateOutputStream delegate) {
        this.delegate = Preconditions.checkNotNull(delegate);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void write(int b) throws IOException {
        delegate.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        delegate.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        delegate.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        // users should not be able to actually close the stream, it is closed by the system.
        // TODO if we want to support async writes, this call could trigger a callback to the
        // snapshot context that a handle is available.
    }

    /**
     * Returns a {@link org.apache.flink.util.ResourceGuard.Lease} that prevents closing this
     * stream. To allow the system to close this stream, each of the acquired leases need to call
     * {@link Lease#close()}, on their acquired leases.
     */
    public final ResourceGuard.Lease acquireLease() throws IOException {
        return resourceGuard.acquireResource();
    }

    /** This method should not be public so as to not expose internals to user code. */
    CheckpointStreamFactory.CheckpointStateOutputStream getDelegate() {
        return delegate;
    }

    /**
     * This method should not be public so as to not expose internals to user code. Closes the
     * underlying stream and returns a state handle.
     */
    abstract T closeAndGetHandle() throws IOException;

    StreamStateHandle closeAndGetHandleAfterLeasesReleased() throws IOException {
        try {
            resourceGuard.closeInterruptibly();
            return delegate.closeAndGetHandle();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            delegate.closeAndGetHandle();
            throw new IOException("Interrupted while awaiting handle.", e);
        }
    }
}
