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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.core.state.InternalStateFuture;

/**
 * An internal factory for {@link InternalStateFuture} that build future with necessary context
 * switch and wired with mailbox executor.
 */
public class StateFutureFactory<R, K> {

    private final AsyncExecutionController<R, K> asyncExecutionController;
    private final MailboxExecutor mailboxExecutor;

    StateFutureFactory(
            AsyncExecutionController<R, K> asyncExecutionController,
            MailboxExecutor mailboxExecutor) {
        this.asyncExecutionController = asyncExecutionController;
        this.mailboxExecutor = mailboxExecutor;
    }

    public <OUT> InternalStateFuture<OUT> create(RecordContext<R, K> context) {
        return new ContextStateFutureImpl<>(
                (runnable) ->
                        mailboxExecutor.submit(
                                () -> {
                                    asyncExecutionController.setCurrentContext(context);
                                    runnable.run();
                                },
                                "State callback."),
                context);
    }
}
