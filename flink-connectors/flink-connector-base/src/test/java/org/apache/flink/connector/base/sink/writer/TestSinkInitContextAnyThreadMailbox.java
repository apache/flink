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

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;

/**
 * A mock implementation of a {@code Sink.InitContext} to be used in sink unit tests.
 *
 * <p>The only difference between this and {@link TestSinkInitContext} is that the mailbox thread
 * methods of this context may be accessed from any thread. This is useful for testing fine-grained
 * interleaving of threads that may be in the asynchronous part of {@code submitRequestEntries()} in
 * the concrete sink against new mailbox threads entering {@code write()} in the base sink.
 *
 * <p>However, care must be taken to ensure deadlocks do not form in the test code, since we are
 * artificially allowing multiple mailbox threads, when only one is supposed to exist.
 */
public class TestSinkInitContextAnyThreadMailbox extends TestSinkInitContext {
    @Override
    public MailboxExecutor getMailboxExecutor() {
        return new MailboxExecutorImpl(
                new AnyThreadTaskMailboxImpl(Thread.currentThread()),
                Integer.MAX_VALUE,
                streamTaskActionExecutor);
    }

    private static class AnyThreadTaskMailboxImpl extends TaskMailboxImpl {
        public AnyThreadTaskMailboxImpl(Thread currentThread) {
            super(currentThread);
        }

        @Override
        public boolean isMailboxThread() {
            return true;
        }
    }
}
