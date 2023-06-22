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

package org.apache.flink.connector.datagen.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.typeutils.OutputTypeConfigurable;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * A stream generator function that returns a sequence of elements.
 *
 * <p>This generator function serializes the elements using Flink's type information. That way, any
 * object transport using Java serialization will not be affected by the serializability of the
 * elements.
 *
 * <p><b>NOTE:</b> This source has a parallelism of 1.
 *
 * @param <OUT> The type of elements returned by this function.
 */
@Internal
public class FromElementsGeneratorFunction<OUT>
        implements GeneratorFunction<Long, OUT>, OutputTypeConfigurable<OUT> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FromElementsGeneratorFunction.class);

    /** The (de)serializer to be used for the data elements. */
    private @Nullable TypeSerializer<OUT> serializer;

    /** The actual data elements, in serialized form. */
    private byte[] elementsSerialized;

    /** The number of elements emitted already. */
    private int numElementsEmitted;

    private final transient Iterable<OUT> elements;
    private transient DataInputView input;

    public FromElementsGeneratorFunction(TypeSerializer<OUT> serializer, OUT... elements)
            throws IOException {
        this(serializer, Arrays.asList(elements));
    }

    public FromElementsGeneratorFunction(TypeSerializer<OUT> serializer, Iterable<OUT> elements)
            throws IOException {
        this.serializer = Preconditions.checkNotNull(serializer);
        this.elements = elements;
        serializeElements();
    }

    @SafeVarargs
    public FromElementsGeneratorFunction(OUT... elements) {
        this(Arrays.asList(elements));
    }

    public FromElementsGeneratorFunction(Iterable<OUT> elements) {
        this.serializer = null;
        this.elements = elements;
        checkIterable(elements, Object.class);
    }

    @VisibleForTesting
    @Nullable
    public TypeSerializer<OUT> getSerializer() {
        return serializer;
    }

    private void serializeElements() throws IOException {
        Preconditions.checkState(serializer != null, "serializer not set");
        LOG.info("Serializing elements using  " + serializer);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);

        try {
            for (OUT element : elements) {
                serializer.serialize(element, wrapper);
            }
        } catch (Exception e) {
            throw new IOException("Serializing the source elements failed: " + e.getMessage(), e);
        }
        this.elementsSerialized = baos.toByteArray();
    }

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
        this.input = new DataInputViewStreamWrapper(bais);
    }

    @Override
    public OUT map(Long nextIndex) throws Exception {
        // Move iterator to the required position in case of failure recovery
        while (numElementsEmitted < nextIndex) {
            numElementsEmitted++;
            tryDeserialize(serializer, input);
        }
        numElementsEmitted++;
        return tryDeserialize(serializer, input);
    }

    private OUT tryDeserialize(TypeSerializer<OUT> serializer, DataInputView input)
            throws IOException {
        try {
            return serializer.deserialize(input);
        } catch (Exception e) {
            throw new IOException(
                    "Failed to deserialize an element from the source. "
                            + "If you are using user-defined serialization (Value and Writable types), check the "
                            + "serialization functions.\nSerializer is "
                            + serializer,
                    e);
        }
    }

    @Override
    public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
        Preconditions.checkState(
                elements != null,
                "The output type should've been specified before shipping the graph to the cluster");
        checkIterable(elements, outTypeInfo.getTypeClass());
        TypeSerializer<OUT> newSerializer = outTypeInfo.createSerializer(executionConfig);
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
