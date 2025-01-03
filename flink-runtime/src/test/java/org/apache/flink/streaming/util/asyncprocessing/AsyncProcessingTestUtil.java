/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.asyncprocessing;

import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateStreamOperator;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateStreamOperatorV2;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.function.RunnableWithException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/** Utility class for async test harness. */
public class AsyncProcessingTestUtil {
    public static <OUT> void drain(StreamOperator<OUT> operator) {
        if (operator instanceof AbstractAsyncStateStreamOperator) {
            ((AbstractAsyncStateStreamOperator<OUT>) operator).drainStateRequests();
        } else if (operator instanceof AbstractAsyncStateStreamOperatorV2) {
            ((AbstractAsyncStateStreamOperatorV2<OUT>) operator)
                    .getAsyncExecutionController()
                    .drainInflightRecords(0);
        } else {
            throw new IllegalStateException("Operator is not an AsyncStateProcessingOperator");
        }
    }

    public static CompletableFuture<Void> execute(
            ExecutorService executor, RunnableWithException processor) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        executor.execute(
                () -> {
                    try {
                        processor.run();
                        future.complete(null);
                    } catch (Exception e) {
                        // Notify the outside future.
                        future.completeExceptionally(e);
                    }
                });
        return future;
    }

    public static Exception unwrapAsyncException(Exception t) {
        while (t != null
                && t.getCause() != null
                && t.getCause() != t
                && (t instanceof ExecutionException || t instanceof RuntimeException)
                && t.getCause() instanceof Exception) {
            t = (Exception) t.getCause();
        }
        return t;
    }
}
