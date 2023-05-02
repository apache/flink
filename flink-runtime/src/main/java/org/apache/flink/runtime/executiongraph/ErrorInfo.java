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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/** Simple container to hold an exception and the corresponding timestamp. */
public class ErrorInfo implements Serializable {

    private static final long serialVersionUID = -6138942031953594202L;

    /**
     * The exception that we keep holding forever. Has no strong reference to any user-defined code.
     */
    private final SerializedThrowable exception;

    private final long timestamp;

    private final Map<String, String> labels;

    /**
     * Instantiates an {@code ErrorInfo} to cover inconsistent behavior due to FLINK-21376.
     *
     * @param exception The error cause that might be {@code null}.
     * @param timestamp The timestamp the error was noticed.
     * @param labels The labels associated with the error.
     * @return a {@code ErrorInfo} containing a generic {@link FlinkException} in case of a missing
     *     error cause.
     */
    public static ErrorInfo createErrorInfoWithNullableCause(
            @Nullable Throwable exception, long timestamp, Map<String, String> labels) {
        return new ErrorInfo(handleMissingThrowable(exception), timestamp, labels);
    }

    /**
     * Utility method to cover FLINK-21376.
     *
     * @param throwable The actual exception.
     * @return a {@link FlinkException} if no exception was passed.
     */
    public static Throwable handleMissingThrowable(@Nullable Throwable throwable) {
        return throwable != null
                ? throwable
                : new FlinkException(
                        "Unknown cause for Execution failure (this might be caused by FLINK-21376).");
    }

    public ErrorInfo(@Nonnull Throwable exception, long timestamp) {
        this(exception, timestamp, Collections.emptyMap());
    }

    public ErrorInfo(@Nonnull Throwable exception, long timestamp, Map<String, String> labels) {
        Preconditions.checkNotNull(exception);
        Preconditions.checkArgument(timestamp > 0);

        this.exception =
                exception instanceof SerializedThrowable
                        ? (SerializedThrowable) exception
                        : new SerializedThrowable(exception);
        this.timestamp = timestamp;
        this.labels = Preconditions.checkNotNull(labels);
    }

    public ErrorInfo withLabels(Map<String, String> labels) {
        return new ErrorInfo(this.exception, this.timestamp, labels);
    }

    /** Returns the serialized form of the original exception. */
    public SerializedThrowable getException() {
        return exception;
    }

    /**
     * Returns the contained exception as a string.
     *
     * @return failure causing exception as a string, or {@code "(null)"}
     */
    public String getExceptionAsString() {
        return exception.getFullStringifiedStackTrace();
    }

    /**
     * Returns the timestamp for the contained exception.
     *
     * @return timestamp of contained exception, or 0 if no exception was set
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns the labels associated with the exception.
     *
     * @return Map of exception labels
     */
    public Map<String, String> getLabels() {
        return labels;
    }
}
