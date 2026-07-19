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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * Thread-scoped holder for a factory that builds a fallback {@link TypeSerializer} when a {@link
 * TypeSerializerSnapshot} restores itself but a class it depends on is not on the classpath — for
 * example a POJO's declared type, or an Avro record's specific/reflect runtime type.
 *
 * <p><b>This exists solely to support the Flink State Processing API's ability to read state whose
 * original classes are not on the classpath</b> (e.g. converting savepoint state into table rows
 * without the user's job JAR). It must never be set by, or otherwise affect, regular job restores:
 * a {@code TypeSerializerSnapshot} only ever consults this factory after it has already determined
 * — independently of this class — that the class it needs is genuinely missing, and only when a
 * factory has actually been registered. With no factory registered (the default for every job that
 * is not using the State Processing API), behavior is unchanged from before this hook existed: the
 * snapshot fails fast with a {@code ClassNotFoundException} or equivalent.
 *
 * <p>A {@code ThreadLocal} is used because {@link
 * CompositeTypeSerializerSnapshot#restoreSerializer()} eagerly restores all of its nested
 * serializers and offers no hook for substituting one of them, so a POJO or Avro type nested
 * arbitrarily deep inside a composite snapshot (e.g. a list or map serializer snapshot) cannot be
 * reached otherwise.
 */
@Internal
public final class CustomRestoreSerializerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CustomRestoreSerializerFactory.class);

    private static final ThreadLocal<Function<TypeSerializerSnapshot<?>, TypeSerializer<?>>>
            FACTORY = new ThreadLocal<>();

    private CustomRestoreSerializerFactory() {}

    /** Registers the fallback factory for the current thread. */
    public static void set(Function<TypeSerializerSnapshot<?>, TypeSerializer<?>> factory) {
        FACTORY.set(factory);
    }

    /** Returns the fallback factory registered via {@link #set}, or {@code null} if none. */
    public static Function<TypeSerializerSnapshot<?>, TypeSerializer<?>> get() {
        return FACTORY.get();
    }

    /** Clears the fallback factory registered for the current thread. */
    public static void remove() {
        FACTORY.remove();
    }

    /**
     * Resolves {@code className} via {@code classLoader}, or returns {@code null} if it cannot be
     * found and a fallback factory is registered for the current thread.
     *
     * @throws NoClassDefFoundError if the class cannot be found and no fallback factory is
     *     registered.
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> resolveOrNull(String className, ClassLoader classLoader) {
        try {
            return (Class<T>) Class.forName(className, false, classLoader);
        } catch (ClassNotFoundException e) {
            if (get() == null) {
                throw missingClass(className, e);
            }
            LOG.debug(
                    "Class '{}' not found on classpath; a CustomRestoreSerializerFactory is"
                            + " registered to read the data without it.",
                    className);
            return null;
        }
    }

    /**
     * Builds the fallback serializer for a {@code snapshot} whose runtime class, {@code
     * missingClassName}, could not be loaded, using the factory registered via {@link #set}.
     *
     * @throws NoClassDefFoundError if no factory is registered for the current thread.
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeSerializer<T> restoreFallbackSerializer(
            TypeSerializerSnapshot<T> snapshot, String missingClassName) {
        Function<TypeSerializerSnapshot<?>, TypeSerializer<?>> fallback = get();
        if (fallback == null) {
            throw missingClass(missingClassName, new ClassNotFoundException(missingClassName));
        }
        return (TypeSerializer<T>) fallback.apply(snapshot);
    }

    private static NoClassDefFoundError missingClass(
            String className, ClassNotFoundException cause) {
        NoClassDefFoundError error = new NoClassDefFoundError(className);
        error.initCause(cause);
        return error;
    }
}
