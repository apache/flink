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

package org.apache.flink.runtime.rest.handler.async;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Encapsulates the result of an asynchronous operation. Depending on its {@link
 * OperationResultStatus}, it contains either the actual result (if completed successfully), or the
 * cause of failure (if it failed), or none of the two (if still in progress).
 */
public class OperationResult<R> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final OperationResultStatus status;
    @Nullable private final R result;
    @Nullable private final Throwable throwable;

    private OperationResult(OperationResultStatus status, R result, Throwable throwable) {
        this.status = status;
        this.result = result;
        this.throwable = throwable;
    }

    public boolean isFinished() {
        return status == OperationResultStatus.SUCCESS || status == OperationResultStatus.FAILURE;
    }

    public OperationResultStatus getStatus() {
        return status;
    }

    public R getResult() {
        checkNotNull(result);
        return result;
    }

    public Throwable getThrowable() {
        checkNotNull(throwable);
        return throwable;
    }

    public static <R> OperationResult<R> failure(Throwable throwable) {
        checkNotNull(throwable);
        return new OperationResult<>(OperationResultStatus.FAILURE, null, throwable);
    }

    public static <R> OperationResult<R> success(R result) {
        checkNotNull(result);
        return new OperationResult<>(OperationResultStatus.SUCCESS, result, null);
    }

    public static <R> OperationResult<R> inProgress() {
        return new OperationResult<>(OperationResultStatus.IN_PROGRESS, null, null);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        @SuppressWarnings("unchecked")
        OperationResult<R> that = (OperationResult<R>) other;

        return this.status == that.status
                && Objects.equals(this.result, that.result)
                && Objects.equals(this.throwable, that.throwable);
    }
}
