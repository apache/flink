/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * A stream source function that returns a sequence of elements.
 *
 * <p>This source function serializes the elements using Flink's type information. That way, any
 * object transport using Java serialization will not be affected by the serializability of the
 * elements.
 *
 * <p><b>NOTE:</b> This source has a parallelism of 1.
 *
 * @param <T> The type of elements returned by this function.
 */
@PublicEvolving
public class FromElementsFunction<T>
        implements SourceFunction<T>, CheckpointedFunction, OutputTypeConfigurable<T> {

    private static final long serialVersionUID = 1L;

    /** The (de)serializer to be used for the data elements. */
    private @Nullable TypeSerializer<T> serializer;

    /** The actual data elements, in serialized form. */
    private byte[] elementsSerialized;

    /** The number of serialized elements. */
    private final int numElements;

    /** The number of elements emitted already. */
    private volatile int numElementsEmitted;

    /** The number of elements to skip initially. */
    private volatile int numElementsToSkip;

    /** Flag to make the source cancelable. */
    private volatile boolean isRunning = true;

    private final transient Iterable<T> elements;

    private transient ListState<Integer> checkpointedState;

    @SafeVarargs
    public FromElementsFunction(TypeSerializer<T> serializer, T... elements) throws IOException {
        this(serializer, Arrays.asList(elements));
    }

    public FromElementsFunction(TypeSerializer<T> serializer, Iterable<T> elements)
            throws IOException {
        this.serializer = Preconditions.checkNotNull(serializer);
        this.elements = elements;
        this.numElements =
                elements instanceof Collection
                        ? ((Collection<T>) elements).size()
                        : (int) IterableUtils.toStream(elements).count();
        serializeElements();
    }

    @SafeVarargs
    public FromElementsFunction(T... elements) {
        this(Arrays.asList(elements));
    }

    public FromElementsFunction(Iterable<T> elements) {
        this.serializer = null;
        this.elements = elements;
        this.numElements =
                elements instanceof Collection
                        ? ((Collection<T>) elements).size()
                        : (int) IterableUtils.toStream(elements).count();
        checkIterable(elements, Object.class);
    }

    @VisibleForTesting
    @Nullable
    public TypeSerializer<T> getSerializer() {
        return serializer;
    }

    private void serializeElements() throws IOException {
        Preconditions.checkState(serializer != null, "serializer not set");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
        try {
            for (T element : elements) {
                serializer.serialize(element, wrapper);
            }
        } catch (Exception e) {
            throw new IOException("Serializing the source elements failed: " + e.getMessage(), e);
        }
        this.elementsSerialized = baos.toByteArray();
    }

    /**
     * Set element type and re-serialize element if required. Should only be called before
     * serialization/deserialization of this function.
     */
    @Override
    public void setOutputType(TypeInformation<T> outTypeInfo, ExecutionConfig executionConfig) {
        Preconditions.checkState(
                elements != null,
                "The output type should've been specified before shipping the graph to the cluster");
        checkIterable(elements, outTypeInfo.getTypeClass());
        TypeSerializer<T> newSerializer = outTypeInfo.createSerializer(executionConfig);
        if (Objects.equals(serializer, newSerializer)) {
            return;
        }
        serializer = newSerializer;
        try {
            serializeElements();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        Preconditions.checkState(
                this.checkpointedState == null,
                "The " + getClass().getSimpleName() + " has already been initialized.");

        this.checkpointedState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "from-elements-state", IntSerializer.INSTANCE));

        if (context.isRestored()) {
            List<Integer> retrievedStates = new ArrayList<>();
            for (Integer entry : this.checkpointedState.get()) {
                retrievedStates.add(entry);
            }

            // given that the parallelism of the function is 1, we can only have 1 state
            Preconditions.checkArgument(
                    retrievedStates.size() == 1,
                    getClass().getSimpleName() + " retrieved invalid state.");

            this.numElementsToSkip = retrievedStates.get(0);
        }
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        Preconditions.checkState(serializer != null, "serializer not configured");
        ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
        final DataInputView input = new DataInputViewStreamWrapper(bais);

        // if we are restored from a checkpoint and need to skip elements, skip them now.
        int toSkip = numElementsToSkip;
        if (toSkip > 0) {
            try {
                while (toSkip > 0) {
                    serializer.deserialize(input);
                    toSkip--;
                }
            } catch (Exception e) {
                throw new IOException(
                        "Failed to deserialize an element from the source. "
                                + "If you are using user-defined serialization (Value and Writable types), check the "
                                + "serialization functions.\nSerializer is "
                                + serializer,
                        e);
            }

            this.numElementsEmitted = this.numElementsToSkip;
        }

        final Object lock = ctx.getCheckpointLock();

        while (isRunning && numElementsEmitted < numElements) {
            T next;
            try {
                next = serializer.deserialize(input);
            } catch (Exception e) {
                throw new IOException(
                        "Failed to deserialize an element from the source. "
                                + "If you are using user-defined serialization (Value and Writable types), check the "
                                + "serialization functions.\nSerializer is "
                                + serializer,
                        e);
            }

            synchronized (lock) {
                ctx.collect(next);
                numElementsEmitted++;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    /**
     * Gets the number of elements produced in total by this function.
     *
     * @return The number of elements produced in total.
     */
    public int getNumElements() {
        return numElements;
    }

    /**
     * Gets the number of elements emitted so far.
     *
     * @return The number of elements emitted so far.
     */
    public int getNumElementsEmitted() {
        return numElementsEmitted;
    }

    // ------------------------------------------------------------------------
    //  Checkpointing
    // ------------------------------------------------------------------------

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(
                this.checkpointedState != null,
                "The " + getClass().getSimpleName() + " has not been properly initialized.");

        this.checkpointedState.clear();
        this.checkpointedState.add(this.numElementsEmitted);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Verifies that all elements in the collection are non-null, and are of the given class, or a
     * subclass thereof.
     *
     * @param elements The collection to check.
     * @param viewedAs The class to which the elements must be assignable to.
     * @param <OUT> The generic type of the collection to be checked.
     */
    public static <OUT> void checkCollection(Collection<OUT> elements, Class<OUT> viewedAs) {
        checkIterable(elements, viewedAs);
    }

    private static <OUT> void checkIterable(Iterable<OUT> elements, Class<?> viewedAs) {
        for (OUT elem : elements) {
            if (elem == null) {
                throw new IllegalArgumentException("The collection contains a null element");
            }

            if (!viewedAs.isAssignableFrom(elem.getClass())) {
                throw new IllegalArgumentException(
                        "The elements in the collection are not all subclasses of "
                                + viewedAs.getCanonicalName());
            }
        }
    }
}
