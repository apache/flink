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
import org.apache.flink.util.AbstractCloseableRegistry;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

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
public class CloseableRegistry extends AbstractCloseableRegistry<Closeable, Object> {

    private static final Object DUMMY = new Object();

    public CloseableRegistry() {
        super(new LinkedHashMap<>());
    }

    @Override
    protected void doRegister(
            @Nonnull Closeable closeable, @Nonnull Map<Closeable, Object> closeableMap) {
        closeableMap.put(closeable, DUMMY);
    }

    @Override
    protected boolean doUnRegister(
            @Nonnull Closeable closeable, @Nonnull Map<Closeable, Object> closeableMap) {
        return closeableMap.remove(closeable) != null;
    }

    @Override
    protected Collection<Closeable> getReferencesToClose() {
        ArrayList<Closeable> closeablesToClose = new ArrayList<>(closeableToRef.keySet());
        Collections.reverse(closeablesToClose);
        return closeablesToClose;
    }
}
