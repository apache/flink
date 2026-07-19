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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.function.Function;

/**
 * Thread-scoped holder for a factory that builds a fallback {@link TypeSerializer} when a {@link
 * PojoSerializerSnapshot} restores itself but its POJO class is not on the classpath.
 *
 * <p>A {@code ThreadLocal} is used because {@code
 * CompositeTypeSerializerSnapshot#restoreSerializer()} eagerly restores all of its nested
 * serializers and offers no hook for substituting one of them, so POJOs nested arbitrarily deep
 * inside a composite snapshot (e.g. a list or map serializer snapshot) cannot be reached otherwise.
 *
 * <p>Callers set the factory via {@link #set} before triggering a restore and never need to unset
 * it: for a given POJO type, the class is either on the classpath or it isn't.
 */
@Internal
public final class PojoRestoreSerializerFallback {

    private static final ThreadLocal<Function<PojoSerializerSnapshot<?>, TypeSerializer<?>>>
            FACTORY = new ThreadLocal<>();

    private PojoRestoreSerializerFallback() {}

    /** Registers the fallback factory for the current thread. */
    public static void set(Function<PojoSerializerSnapshot<?>, TypeSerializer<?>> factory) {
        FACTORY.set(factory);
    }

    /** Returns the fallback factory registered via {@link #set}, or {@code null} if none. */
    static Function<PojoSerializerSnapshot<?>, TypeSerializer<?>> get() {
        return FACTORY.get();
    }
}
