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

package org.apache.flink.api.common.errors;

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * An exception that can carry a {@link FlinkError}. Implemented by {@code FlinkRuntimeException},
 * {@code FlinkException} and their subclasses.
 *
 * <p>An exception created from an error and its arguments has the error's formatted template as its
 * detail message. An exception created with a free-text message has no error.
 */
@PublicEvolving
public interface FlinkThrowable {

    /** The error, or empty if the exception was created with a free-text message. */
    Optional<FlinkError> getError();

    /** The arguments by parameter name, in declaration order. Empty without an error. */
    Map<String, Object> getErrorArgs();

    /**
     * Returns the argument of the given parameter.
     *
     * @throws IllegalArgumentException if the exception has no argument for the parameter
     */
    @SuppressWarnings("unchecked")
    default <T> T getErrorArg(String name) {
        final Map<String, Object> arguments = getErrorArgs();
        if (!arguments.containsKey(name)) {
            throw new IllegalArgumentException(
                    String.format("The exception has no argument for parameter '%s'.", name));
        }
        return (T) arguments.get(name);
    }

    /** The detail message, see {@link Throwable#getMessage()}. */
    String getMessage();

    /**
     * The detail message with the arguments of sensitive parameters replaced by {@code <redacted>}.
     * Meant for logs. Without an error, the message is free text that can contain user data
     * anywhere, so the whole message is {@code <redacted>}. The same applies if the arguments don't
     * match the parameters of the error.
     */
    default String getRedactedMessage() {
        final Optional<FlinkError> error = getError();
        if (error.isEmpty()) {
            return "<redacted>";
        }
        try {
            return error.get()
                    .formatRedacted(new ArrayList<>(getErrorArgs().values()), "<redacted>");
        } catch (IllegalArgumentException e) {
            // For example, the exception was serialized by a version with other parameters.
            return "<redacted>";
        }
    }

    /** The cause, see {@link Throwable#getCause()}. */
    @Nullable
    Throwable getCause();

    /** Whether the exception carries the given error. */
    default boolean is(FlinkError expected) {
        return getError().map(error -> error == expected).orElse(false);
    }

    /** Whether this exception or one of its causes carries the given error. */
    default boolean containsCause(FlinkError expected) {
        return findCause(expected).isPresent();
    }

    /**
     * Returns the first exception that carries the given error, starting with this exception and
     * following its causes.
     */
    default Optional<FlinkThrowable> findCause(FlinkError expected) {
        return is(expected) ? Optional.of(this) : FlinkThrowable.findCause(getCause(), expected);
    }

    /** Returns the given exception as a {@link FlinkThrowable}, or empty if it isn't one. */
    static Optional<FlinkThrowable> get(@Nullable Throwable throwable) {
        return throwable instanceof FlinkThrowable
                ? Optional.of((FlinkThrowable) throwable)
                : Optional.empty();
    }

    /**
     * Returns the given exception as a {@link FlinkThrowable} if it carries the given error, or
     * empty otherwise.
     */
    static Optional<FlinkThrowable> get(@Nullable Throwable throwable, FlinkError expected) {
        return get(throwable).filter(flinkThrowable -> flinkThrowable.is(expected));
    }

    /**
     * Returns the first exception that carries the given error, starting with the given exception
     * and following its causes.
     */
    static Optional<FlinkThrowable> findCause(@Nullable Throwable throwable, FlinkError expected) {
        final Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = throwable;
        while (current != null && visited.add(current)) {
            final Optional<FlinkThrowable> found = get(current, expected);
            if (found.isPresent()) {
                return found;
            }
            current = current.getCause();
        }
        return Optional.empty();
    }

    /** Returns the given exception as a {@link FlinkThrowable}, or rethrows it if it isn't one. */
    static <E extends Throwable> FlinkThrowable getOrThrow(E throwable) throws E {
        return get(throwable).orElseThrow(() -> throwable);
    }

    /**
     * Returns the given exception as a {@link FlinkThrowable} if it carries the given error, or
     * rethrows it otherwise.
     */
    static <E extends Throwable> FlinkThrowable getOrThrow(E throwable, FlinkError expected)
            throws E {
        return get(throwable, expected).orElseThrow(() -> throwable);
    }

    /**
     * Returns the first exception that carries the given error, starting with the given exception
     * and following its causes, or rethrows the given exception if there is none.
     */
    static <E extends Throwable> FlinkThrowable findCauseOrThrow(E throwable, FlinkError expected)
            throws E {
        return findCause(throwable, expected).orElseThrow(() -> throwable);
    }
}
