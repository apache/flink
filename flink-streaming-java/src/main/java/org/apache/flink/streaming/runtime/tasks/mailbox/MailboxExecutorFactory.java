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

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;

/**
 * A factory for creating mailbox executors with a given priority. The factory is usually bound to a
 * specific task.
 */
@FunctionalInterface
@Internal
public interface MailboxExecutorFactory {

    /**
     * Creates a new executor for the given priority. The priority is used when enqueuing new mails
     * as well as yielding.
     *
     * @param priority the priority of the mailbox executor.
     * @return a mailbox executor with the bound priority.
     */
    MailboxExecutor createExecutor(int priority);
}
