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

package org.apache.flink.table.gateway;

import org.apache.flink.util.IOUtils;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static org.apache.flink.shaded.guava31.com.google.common.collect.Lists.reverse;

/**
 * Simplify {@link org.apache.flink.core.fs.AutoCloseableRegistry}. Note: with the class, the
 * flink-jdbc-module doesn't rely on the flink-core module.
 */
public class AutoCloseableRegistry implements AutoCloseable {

    /** Lock that guards state of this registry. * */
    private final Object lock;

    /** Map from tracked Closeables to some associated meta data. */
    @GuardedBy("lock")
    protected final LinkedHashSet<AutoCloseable> closeableToRef;

    /** Indicates if this registry is closed. */
    @GuardedBy("lock")
    private boolean closed;

    public AutoCloseableRegistry() {
        this.lock = new Object();
        this.closeableToRef = new LinkedHashSet<>();
        this.closed = false;
    }

    /**
     * Registers a {@link AutoCloseable} with the registry. In case the registry is already closed,
     * this method throws an {@link IllegalStateException} and closes the passed {@link
     * AutoCloseable}.
     *
     * @param closeable Closeable to register.
     * @throws IOException exception when the registry was closed before.
     */
    public final void registerCloseable(AutoCloseable closeable) throws IOException {

        if (null == closeable) {
            return;
        }

        synchronized (lock) {
            if (!closed) {
                closeableToRef.add(closeable);
                return;
            }
        }

        IOUtils.closeQuietly(closeable);
        throw new IOException(
                "Cannot register Closeable, registry is already closed. Closing argument.");
    }

    /**
     * Removes a {@link Closeable} from the registry.
     *
     * @param closeable instance to remove from the registry.
     * @return true if the closeable was previously registered and became unregistered through this
     *     call.
     */
    public final boolean unregisterCloseable(AutoCloseable closeable) {

        if (null == closeable) {
            return false;
        }

        synchronized (lock) {
            return closeableToRef.remove(closeable);
        }
    }

    @Override
    public void close() throws Exception {
        List<AutoCloseable> toCloseCopy;

        synchronized (lock) {
            if (closed) {
                return;
            }

            closed = true;

            toCloseCopy = new ArrayList<>(closeableToRef);

            closeableToRef.clear();
        }

        IOUtils.closeAllQuietly(reverse(toCloseCopy));
    }

    public boolean isClosed() {
        synchronized (lock) {
            return closed;
        }
    }
}
