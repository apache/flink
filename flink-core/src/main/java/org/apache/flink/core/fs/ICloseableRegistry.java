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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class allows to register instances of {@link Closeable}, which are all closed if this
 * registry is closed.
 *
 * <p>Registering to an already closed registry will throw an exception and close the provided
 * {@link Closeable}
 *
 * <p>All methods in this class are thread-safe.
 *
 * <p>This class closes all registered {@link Closeable}s in the reverse registration order.
 */
@Internal
public interface ICloseableRegistry extends Closeable {

    static Closeable asCloseable(AutoCloseable autoCloseable) {
        AtomicBoolean closed = new AtomicBoolean(false);
        return () -> {
            if (closed.compareAndSet(false, true)) {
                try {
                    autoCloseable.close();
                } catch (Exception e) {
                    ExceptionUtils.rethrowIOException(e);
                }
            }
        };
    }

    /**
     * Registers a {@link Closeable} with the registry. In case the registry is already closed, this
     * method throws an {@link IllegalStateException} and closes the passed {@link Closeable}.
     *
     * @param closeable Closeable to register.
     * @throws IOException exception when the registry was closed before.
     */
    void registerCloseable(Closeable closeable) throws IOException;

    /**
     * Same as {@link #registerCloseable(Closeable)} but allows to {@link
     * #unregisterCloseable(Closeable) unregister} the passed closeable by closing the returned
     * closeable.
     *
     * @param closeable Closeable to register.
     * @return another Closeable that unregisters the passed closeable.
     * @throws IOException exception when the registry was closed before.
     */
    default Closeable registerCloseableTemporarily(Closeable closeable) throws IOException {
        registerCloseable(closeable);
        return () -> unregisterCloseable(closeable);
    }

    /**
     * Removes a {@link Closeable} from the registry.
     *
     * @param closeable instance to remove from the registry.
     * @return true if the closeable was previously registered and became unregistered through this
     *     call.
     */
    boolean unregisterCloseable(Closeable closeable);

    /** No-op implementation of {@link org.apache.flink.core.fs.ICloseableRegistry}. */
    ICloseableRegistry NO_OP =
            new ICloseableRegistry() {
                @Override
                public void registerCloseable(Closeable closeable) {}

                @Override
                public boolean unregisterCloseable(Closeable closeable) {
                    return false;
                }

                @Override
                public boolean isClosed() {
                    return false;
                }

                @Override
                public void close() {}
            };

    /** @return true if this registry was closed. */
    boolean isClosed();
}
