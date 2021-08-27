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

package org.apache.flink.runtime.broadcast;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class represents a single materialization of a broadcast variable and maintains a reference
 * count for it. If the reference count reaches zero the variable is no longer accessible and will
 * eventually be garbage-collected.
 *
 * @param <T> The type of the elements in the broadcast data set.
 */
public class BroadcastVariableMaterialization<T, C> {

    private static final Logger LOG =
            LoggerFactory.getLogger(BroadcastVariableMaterialization.class);

    private final Set<BatchTask<?, ?>> references = new HashSet<BatchTask<?, ?>>();

    private final Object materializationMonitor = new Object();

    private final BroadcastVariableKey key;

    private ArrayList<T> data;

    private C transformed;

    private boolean materialized;

    private boolean disposed;

    public BroadcastVariableMaterialization(BroadcastVariableKey key) {
        this.key = key;
    }

    // --------------------------------------------------------------------------------------------

    public void materializeVariable(
            MutableReader<?> reader,
            TypeSerializerFactory<?> serializerFactory,
            BatchTask<?, ?> referenceHolder)
            throws MaterializationExpiredException, IOException {
        Preconditions.checkNotNull(reader);
        Preconditions.checkNotNull(serializerFactory);
        Preconditions.checkNotNull(referenceHolder);

        final boolean materializer;

        // hold the reference lock only while we track references and decide who should be the
        // materializer
        // that way, other tasks can de-register (in case of failure) while materialization is
        // happening
        synchronized (references) {
            if (disposed) {
                throw new MaterializationExpiredException();
            }

            // sanity check
            if (!references.add(referenceHolder)) {
                throw new IllegalStateException(
                        String.format(
                                "The task %s already holds a reference to the broadcast variable %s.",
                                referenceHolder
                                        .getEnvironment()
                                        .getTaskInfo()
                                        .getTaskNameWithSubtasks(),
                                key.toString()));
            }

            materializer = references.size() == 1;
        }

        try {
            @SuppressWarnings("unchecked")
            final MutableReader<DeserializationDelegate<T>> typedReader =
                    (MutableReader<DeserializationDelegate<T>>) reader;

            @SuppressWarnings("unchecked")
            final TypeSerializer<T> serializer =
                    ((TypeSerializerFactory<T>) serializerFactory).getSerializer();

            final ReaderIterator<T> readerIterator = new ReaderIterator<T>(typedReader, serializer);

            if (materializer) {
                // first one, so we need to materialize;
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Getting Broadcast Variable ("
                                    + key
                                    + ") - First access, materializing.");
                }

                ArrayList<T> data = new ArrayList<T>();

                T element;
                while ((element = readerIterator.next()) != null) {
                    data.add(element);
                }

                synchronized (materializationMonitor) {
                    this.data = data;
                    this.materialized = true;
                    materializationMonitor.notifyAll();
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Materialization of Broadcast Variable (" + key + ") finished.");
                }
            } else {
                // successor: discard all data and refer to the shared variable

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Getting Broadcast Variable (" + key + ") - shared access.");
                }

                T element = serializer.createInstance();
                while ((element = readerIterator.next(element)) != null) {}

                synchronized (materializationMonitor) {
                    while (!this.materialized && !disposed) {
                        materializationMonitor.wait();
                    }
                }
            }
        } catch (Throwable t) {
            // in case of an exception, we need to clean up big time
            decrementReferenceIfHeld(referenceHolder);

            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new IOException("Materialization of the broadcast variable failed.", t);
            }
        }
    }

    public boolean decrementReference(BatchTask<?, ?> referenceHolder) {
        return decrementReferenceInternal(referenceHolder, true);
    }

    public boolean decrementReferenceIfHeld(BatchTask<?, ?> referenceHolder) {
        return decrementReferenceInternal(referenceHolder, false);
    }

    private boolean decrementReferenceInternal(
            BatchTask<?, ?> referenceHolder, boolean errorIfNoReference) {
        synchronized (references) {
            if (disposed || references.isEmpty()) {
                if (errorIfNoReference) {
                    throw new IllegalStateException(
                            "Decrementing reference to broadcast variable that is no longer alive.");
                } else {
                    return false;
                }
            }

            if (!references.remove(referenceHolder)) {
                if (errorIfNoReference) {
                    throw new IllegalStateException(
                            String.format(
                                    "The task %s did not hold a reference to the broadcast variable %s.",
                                    referenceHolder
                                            .getEnvironment()
                                            .getTaskInfo()
                                            .getTaskNameWithSubtasks(),
                                    key.toString()));
                } else {
                    return false;
                }
            }

            if (references.isEmpty()) {
                disposed = true;
                data = null;
                transformed = null;
                return true;
            } else {
                return false;
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    public List<T> getVariable() throws InitializationTypeConflictException {
        if (!materialized) {
            throw new IllegalStateException(
                    "The Broadcast Variable has not yet been materialized.");
        }
        if (disposed) {
            throw new IllegalStateException("The Broadcast Variable has been disposed");
        }

        synchronized (references) {
            if (transformed != null) {
                if (transformed instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<T> casted = (List<T>) transformed;
                    return casted;
                } else {
                    throw new InitializationTypeConflictException(transformed.getClass());
                }
            } else {
                return data;
            }
        }
    }

    public C getVariable(BroadcastVariableInitializer<T, C> initializer) {
        if (!materialized) {
            throw new IllegalStateException(
                    "The Broadcast Variable has not yet been materialized.");
        }
        if (disposed) {
            throw new IllegalStateException("The Broadcast Variable has been disposed");
        }

        synchronized (references) {
            if (transformed == null) {
                transformed = initializer.initializeBroadcastVariable(data);
                data = null;
            }
            return transformed;
        }
    }
}
