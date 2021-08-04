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

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.FlinkRuntimeException;

import javax.transaction.xa.Xid;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Internal
interface XaGroupOps extends Serializable {

    GroupXaOperationResult<CheckpointAndXid> commit(
            List<CheckpointAndXid> xids, boolean allowOutOfOrderCommits, int maxCommitAttempts);

    GroupXaOperationResult<Xid> failOrRollback(Collection<Xid> xids);

    void recoverAndRollback(RuntimeContext runtimeContext, XidGenerator xidGenerator);

    class GroupXaOperationResult<T> {
        private final List<T> succeeded = new ArrayList<>();
        private final List<T> failed = new ArrayList<>();
        private final List<T> toRetry = new ArrayList<>();
        private Optional<Exception> failure = Optional.empty();
        private Optional<Exception> transientFailure = Optional.empty();

        void failedTransiently(T x, XaFacade.TransientXaException e) {
            toRetry.add(x);
            transientFailure =
                    getTransientFailure().isPresent() ? getTransientFailure() : Optional.of(e);
        }

        void failed(T x, Exception e) {
            failed.add(x);
            failure = failure.isPresent() ? failure : Optional.of(e);
        }

        void succeeded(T x) {
            succeeded.add(x);
        }

        private FlinkRuntimeException wrapFailure(
                Exception error, String formatWithCounts, int errCount) {
            return new FlinkRuntimeException(
                    String.format(formatWithCounts, errCount, total()), error);
        }

        private int total() {
            return succeeded.size() + failed.size() + toRetry.size();
        }

        List<T> getForRetry() {
            return toRetry;
        }

        Optional<Exception> getTransientFailure() {
            return transientFailure;
        }

        boolean hasNoFailures() {
            return !failure.isPresent() && !transientFailure.isPresent();
        }

        void throwIfAnyFailed(String action) {
            failure.map(
                            f ->
                                    wrapFailure(
                                            f,
                                            "failed to " + action + " %d transactions out of %d",
                                            toRetry.size() + failed.size()))
                    .ifPresent(
                            f -> {
                                throw f;
                            });
        }
    }
}
