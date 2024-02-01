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
import org.apache.flink.util.AbstractAutoCloseableRegistry;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.shaded.guava31.com.google.common.collect.Lists.reverse;

/**
 * This class allows to register instances of {@link AutoCloseable}, which are all closed if this
 * registry is closed.
 *
 * <p>Registering to an already closed registry will throw an exception and close the provided
 * {@link AutoCloseable}.
 *
 * <p>Unlike {@link CloseableRegistry} this class can throw an exception during the close.
 *
 * <p>This class closes all registered {@link Closeable}s in the reverse registration order.
 */
@ThreadSafe
@Internal
public class AutoCloseableRegistry
        extends AbstractAutoCloseableRegistry<AutoCloseable, AutoCloseable, Object, Exception> {

    private static final Object DUMMY = new Object();

    public AutoCloseableRegistry() {
        super(new LinkedHashMap<>());
    }

    @Override
    protected void doRegister(
            @Nonnull AutoCloseable closeable, @Nonnull Map<AutoCloseable, Object> closeableMap) {
        closeableMap.put(closeable, DUMMY);
    }

    @Override
    protected boolean doUnRegister(
            @Nonnull AutoCloseable closeable, @Nonnull Map<AutoCloseable, Object> closeableMap) {
        return closeableMap.remove(closeable) != null;
    }

    /** This implementation implies that any exception is possible during closing. */
    @Override
    protected void doClose(List<AutoCloseable> toClose) throws Exception {
        IOUtils.closeAll(reverse(toClose), Throwable.class);
    }
}
