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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.core.state.StateFutureImpl.AsyncFrameworkExceptionHandler;

/**
 * An internal factory for {@link InternalStateFuture} that build future with necessary context
 * switch and wired with mailbox executor.
 */
public class StateFutureFactory<K> {

    private final AsyncExecutionController<K> asyncExecutionController;
    private final CallbackRunnerWrapper callbackRunner;
    private final AsyncFrameworkExceptionHandler exceptionHandler;

    StateFutureFactory(
            AsyncExecutionController<K> asyncExecutionController,
            CallbackRunnerWrapper callbackRunner,
            AsyncFrameworkExceptionHandler exceptionHandler) {
        this.asyncExecutionController = asyncExecutionController;
        this.callbackRunner = callbackRunner;
        this.exceptionHandler = exceptionHandler;
    }

    public <OUT> InternalStateFuture<OUT> create(RecordContext<K> context) {
        return new ContextStateFutureImpl<>(
                (runnable) ->
                        callbackRunner.submit(
                                () -> {
                                    asyncExecutionController.setCurrentContext(context);
                                    runnable.run();
                                }),
                exceptionHandler,
                context);
    }
}
