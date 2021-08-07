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

package org.apache.flink.runtime.concurrent.akka;

import akka.dispatch.OnComplete;

import java.util.concurrent.CompletableFuture;

import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

/** Utilities to convert Scala types into Java types. */
public class AkkaFutureUtils {
    /**
     * Converts a Scala {@link Future} to a {@link CompletableFuture}.
     *
     * @param scalaFuture to convert to a Java 8 CompletableFuture
     * @param <T> type of the future value
     * @param <U> type of the original future
     * @return Java 8 CompletableFuture
     */
    public static <T, U extends T> CompletableFuture<T> toJava(Future<U> scalaFuture) {
        final CompletableFuture<T> result = new CompletableFuture<>();

        scalaFuture.onComplete(
                new OnComplete<U>() {
                    @Override
                    public void onComplete(Throwable failure, U success) {
                        if (failure != null) {
                            result.completeExceptionally(failure);
                        } else {
                            result.complete(success);
                        }
                    }
                },
                DirectExecutionContext.INSTANCE);

        return result;
    }

    /** Direct execution context. */
    private static class DirectExecutionContext implements ExecutionContext {

        static final DirectExecutionContext INSTANCE = new DirectExecutionContext();

        private DirectExecutionContext() {}

        @Override
        public void execute(Runnable runnable) {
            runnable.run();
        }

        @Override
        public void reportFailure(Throwable cause) {
            throw new IllegalStateException("Error in direct execution context.", cause);
        }

        @Override
        public ExecutionContext prepare() {
            return this;
        }
    }
}
