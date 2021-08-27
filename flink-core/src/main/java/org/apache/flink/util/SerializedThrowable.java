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

    /** The original exception in serialized form. */
    private final byte[] serializedException;

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
        super(getMessageOrError(exception));

        if (!(exception instanceof SerializedThrowable)) {
            // serialize and memoize the original message
            byte[] serialized;
            try {
                serialized = InstantiationUtil.serializeObject(exception);
            } catch (Throwable t) {
                serialized = null;
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

        } else {
            // copy from that serialized throwable
            SerializedThrowable other = (SerializedThrowable) exception;
            this.serializedException = other.serializedException;
            this.originalErrorClassName = other.originalErrorClassName;
            this.fullStringifiedStackTrace = other.fullStringifiedStackTrace;
            this.cachedException = other.cachedException;
            this.setStackTrace(other.getStackTrace());
            this.initCause(other.getCause());
        }
    }

    public Throwable deserializeError(ClassLoader classloader) {
        if (serializedException == null) {
            // failed to serialize the original exception
            // return this SerializedThrowable as a stand in
            return this;
        }

        Throwable cached = cachedException == null ? null : cachedException.get();
        if (cached == null) {
            try {
                cached = InstantiationUtil.deserializeObject(serializedException, classloader);
                cachedException = new WeakReference<>(cached);
            } catch (Throwable t) {
                // something went wrong
                // return this SerializedThrowable as a stand in
                return this;
            }
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

    private static String getMessageOrError(Throwable error) {
        try {
            return error.getMessage();
        } catch (Throwable t) {
            return "(failed to get message)";
        }
    }
}
