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

package org.apache.flink.streaming.api.operators.python.embedded;

import org.apache.flink.annotation.Internal;

import java.lang.reflect.Method;
import java.util.Objects;

/**
 * Reflective adapter for embedded Python iterators.
 *
 * <p>PEMJA iterator objects may come back from a different user-code classloader after recovery, so
 * callers should not hard-cast them to {@code pemja.core.object.PyIterator}.
 */
@Internal
public final class EmbeddedPythonIterator implements AutoCloseable {

    private final Object iterator;
    private final Method hasNextMethod;
    private final Method nextMethod;
    private final Method closeMethod;

    private EmbeddedPythonIterator(Object iterator) {
        this.iterator = Objects.requireNonNull(iterator, "iterator must not be null");

        try {
            Class<?> iteratorClass = iterator.getClass();
            this.hasNextMethod = iteratorClass.getMethod("hasNext");
            this.nextMethod = iteratorClass.getMethod("next");
            this.closeMethod = iteratorClass.getMethod("close");
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to adapt embedded Python iterator of type %s.",
                            iterator.getClass().getName()),
                    e);
        }
    }

    public static EmbeddedPythonIterator from(Object iterator) {
        return new EmbeddedPythonIterator(iterator);
    }

    public boolean hasNext() throws Exception {
        return (boolean) hasNextMethod.invoke(iterator);
    }

    public Object next() throws Exception {
        return nextMethod.invoke(iterator);
    }

    @Override
    public void close() throws Exception {
        closeMethod.invoke(iterator);
    }
}
