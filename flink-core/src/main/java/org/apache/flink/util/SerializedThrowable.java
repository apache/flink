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

package org.apache.flink.util;

import javax.annotation.Nullable;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility class for dealing with user-defined Throwable types that are serialized (for example
 * during RPC/Actor communication), but cannot be resolved with the default class loader.
 *
 * <p>This exception mimics the original exception with respect to message and stack trace, and
 * contains the original exception in serialized form. The original exception can be re-obtained by
 * supplying the appropriate class loader.
 */
public class SerializedThrowable extends Exception implements Serializable {

    private static final long serialVersionUID = 7284183123441947635L;

    /** The original exception's own serialized bytes, or {@code null} if unavailable. */
    private final byte[] serializedException;

    /**
     * Bytes of the whole {@code SerializedThrowable} wrapper as it arrived on the wire (see the
     * wire-safe constructor below), or {@code null} for a self-produced instance. Used only
     * internally by {@link #deserializeError}, as a fallback when {@link #serializedException} is
     * {@code null}: unlike that field, this is not, by itself, the original exception's own
     * serialized form, so it is never exposed through {@link #getSerializedException()}.
     */
    private final byte[] wireWrapperBytes;

    /** Name of the original error class. */
    private final String originalErrorClassName;

    /** The original stack trace, to be printed. */
    private final String fullStringifiedStackTrace;

    /**
     * The original exception, not transported via serialization, because the class may not be part
     * of the system class loader. In addition, we make sure our cached references to not prevent
     * unloading the exception class.
     */
    private transient WeakReference<Throwable> cachedException;

    /**
     * Create a new SerializedThrowable.
     *
     * @param exception The exception to serialize.
     */
    public SerializedThrowable(Throwable exception) {
        this(exception, new HashSet<>());
    }

    private SerializedThrowable(Throwable exception, Set<Throwable> alreadySeen) {
        // When copying from an already-SerializedThrowable value (the else branch below), its own
        // getMessage() is already correctly formatted ("<originalClassName>: <message>") from
        // whenever it was itself constructed - reuse it verbatim instead of recomputing this via
        // getClassNameAndMessageOrError(exception), which would stamp SerializedThrowable's own
        // class name instead of the originally wrapped exception's.
        super(
                exception instanceof SerializedThrowable
                        ? exception.getMessage()
                        : getClassNameAndMessageOrError(exception));

        if (!(exception instanceof SerializedThrowable)) {
            this.wireWrapperBytes = null;

            // serialize and memoize the original message
            byte[] serialized;
            // introduce the synchronization here to avoid deadlock of multi thread serializing
            // exceptions
            synchronized (SerializedThrowable.class) {
                try {
                    serialized = InstantiationUtil.serializeObject(exception);
                } catch (Throwable t) {
                    serialized = null;
                }
            }
            this.serializedException = serialized;
            this.cachedException = new WeakReference<>(exception);

            // record the original exception's properties (name, stack prints)
            this.originalErrorClassName = exception.getClass().getName();
            this.fullStringifiedStackTrace = ExceptionUtils.stringifyException(exception);

            // mimic the original exception's stack trace
            setStackTrace(exception.getStackTrace());

            // mimic the original exception's cause
            if (exception.getCause() == null) {
                initCause(null);
            } else {
                // exception causes may by cyclic, so we truncate the cycle when we find it
                if (alreadySeen.add(exception)) {
                    // we are not in a cycle, yet
                    initCause(new SerializedThrowable(exception.getCause(), alreadySeen));
                }
            }
            // mimic suppressed exceptions
            this.addAllSuppressed(exception.getSuppressed(), alreadySeen);
        } else {
            // copy from that serialized throwable
            SerializedThrowable other = (SerializedThrowable) exception;
            this.serializedException = other.serializedException;
            this.wireWrapperBytes = other.wireWrapperBytes;
            this.originalErrorClassName = other.originalErrorClassName;
            this.fullStringifiedStackTrace = other.fullStringifiedStackTrace;
            this.cachedException = other.cachedException;
            this.setStackTrace(other.getStackTrace());
            this.initCause(other.getCause());
            this.addAllSuppressed(other.getSuppressed(), alreadySeen);
        }
    }

    /**
     * Caps how many nested {@link SerializedThrowable} layers {@link #deserializeError} will
     * unwrap. A well-formed instance never nests more than one or two layers deep (see the
     * unwrapping comment in {@link #deserializeError}); this only guards against an unexpectedly
     * long chain turning an explicit, trusted {@code deserializeError()} call into unbounded
     * recursion.
     */
    private static final int MAX_DESERIALIZE_UNWRAP_DEPTH = 100;

    public Throwable deserializeError(ClassLoader classloader) {
        return deserializeError(classloader, 0);
    }

    private Throwable deserializeError(ClassLoader classloader, int depth) {
        final byte[] bytesToDeserialize =
                serializedException != null ? serializedException : wireWrapperBytes;
        if (bytesToDeserialize == null) {
            // failed to serialize the original exception, and this isn't a wire-reconstructed
            // instance either
            // return this SerializedThrowable as a stand in
            return this;
        }
        if (depth >= MAX_DESERIALIZE_UNWRAP_DEPTH) {
            return this;
        }

        Throwable cached = cachedException == null ? null : cachedException.get();
        if (cached == null) {
            try {
                cached = InstantiationUtil.deserializeObject(bytesToDeserialize, classloader);
                cachedException = new WeakReference<>(cached);
            } catch (Throwable t) {
                // something went wrong
                // return this SerializedThrowable as a stand in
                return this;
            }
        }
        // wireWrapperBytes (used above when serializedException is null) is the bytes of the
        // whole SerializedThrowable this instance arrived in (kept on the wire for old-client
        // compatibility, see SerializedThrowableSerializer), not just the original exception's
        // own bytes. Fully deserializing those bytes yields another SerializedThrowable whose own
        // serializedException field is the original exception's bytes; unwrap until we reach the
        // real object, exactly as old clients did by fully deserializing in one step.
        if (cached instanceof SerializedThrowable && cached != this) {
            return ((SerializedThrowable) cached).deserializeError(classloader, depth + 1);
        }
        return cached;
    }

    public String getOriginalErrorClassName() {
        return originalErrorClassName;
    }

    public byte[] getSerializedException() {
        return serializedException;
    }

    public String getFullStringifiedStackTrace() {
        return fullStringifiedStackTrace;
    }

    /**
     * Add all suppressed exceptions to this exception.
     *
     * @param suppressed The suppressed exceptions to add.
     * @param alreadySeen The set of exceptions that have already been seen.
     */
    private void addAllSuppressed(Throwable[] suppressed, Set<Throwable> alreadySeen) {
        for (Throwable s : suppressed) {
            if (alreadySeen.add(s)) {
                SerializedThrowable serializedThrowable;
                if (s instanceof SerializedThrowable) {
                    serializedThrowable = (SerializedThrowable) s;
                } else {
                    serializedThrowable = new SerializedThrowable(s);
                }
                this.addSuppressed(serializedThrowable);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Override the behavior of Throwable
    // ------------------------------------------------------------------------

    @Override
    public void printStackTrace(PrintStream s) {
        s.print(fullStringifiedStackTrace);
        s.flush();
    }

    @Override
    public void printStackTrace(PrintWriter s) {
        s.print(fullStringifiedStackTrace);
        s.flush();
    }

    @Override
    public String toString() {
        String message = getLocalizedMessage();
        return (message != null)
                ? (originalErrorClassName + ": " + message)
                : originalErrorClassName;
    }

    // ------------------------------------------------------------------------
    //  Static utilities
    // ------------------------------------------------------------------------

    public static Throwable get(Throwable serThrowable, ClassLoader loader) {
        if (serThrowable instanceof SerializedThrowable) {
            return ((SerializedThrowable) serThrowable).deserializeError(loader);
        } else {
            return serThrowable;
        }
    }

    /**
     * Constructs a SerializedThrowable directly from its already-serialized wire representation
     * (see {@code SerializedThrowableDeserializer}), without deserializing {@code wireWrapperBytes}
     * - that only happens lazily, on an explicit {@link #deserializeError} call. Used when
     * reconstructing a SerializedThrowable that arrived over a channel this process does not fully
     * control (e.g. a REST response), where those bytes should not be deserialized automatically.
     *
     * @param message the message to report via {@link #getMessage()}, normally {@code "<class>:
     *     <original message>"} to match {@link #SerializedThrowable(Throwable)}
     * @param originalErrorClassName name of the original exception's class
     * @param fullStringifiedStackTrace the original exception's stringified stack trace
     * @param wireWrapperBytes bytes usable by {@link #deserializeError} to recover the original
     *     exception, or {@code null} if unavailable; not touched by this constructor. See {@link
     *     #wireWrapperBytes}'s Javadoc for why these are kept separate from {@link
     *     #serializedException} and never returned by {@link #getSerializedException()}.
     */
    public SerializedThrowable(
            @Nullable String message,
            String originalErrorClassName,
            String fullStringifiedStackTrace,
            @Nullable byte[] wireWrapperBytes) {
        super(message);
        this.serializedException = null;
        this.wireWrapperBytes = wireWrapperBytes;
        this.originalErrorClassName = originalErrorClassName;
        this.fullStringifiedStackTrace = fullStringifiedStackTrace;
        this.cachedException = null;
        // super(message) fills in this constructor's own call stack by default; the original
        // exception's stack trace is only available as text, in fullStringifiedStackTrace, so
        // there is nothing meaningful to put here structurally.
        setStackTrace(new StackTraceElement[0]);
    }

    private static String getClassNameAndMessageOrError(Throwable error) {
        try {
            String className = error.getClass().getName();
            String message = error.getMessage();
            if (message != null) {
                return String.format("%s: %s", className, message);
            }
            return className;
        } catch (Throwable t) {
            return "(failed to get message)";
        }
    }
}
