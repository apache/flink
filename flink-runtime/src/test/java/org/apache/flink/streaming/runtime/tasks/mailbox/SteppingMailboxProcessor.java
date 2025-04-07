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

import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;

import java.util.Optional;

import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.MIN_PRIORITY;

/** A {@link MailboxProcessor} that allows to execute one mail at a time. */
public class SteppingMailboxProcessor extends MailboxProcessor {
    public SteppingMailboxProcessor(
            MailboxDefaultAction mailboxDefaultAction,
            TaskMailbox mailbox,
            StreamTaskActionExecutor actionExecutor) {
        super(mailboxDefaultAction, mailbox, actionExecutor);
    }

    @Override
    public boolean runMailboxStep() throws Exception {
        assert mailbox.getState() == TaskMailbox.State.OPEN : "Mailbox must be opened!";
        final MailboxController defaultActionContext = new MailboxController(this);

        Optional<Mail> maybeMail;
        if (isMailboxLoopRunning() && (maybeMail = mailbox.tryTake(MIN_PRIORITY)).isPresent()) {
            maybeMail.get().run();
            return true;
        }
        mailboxDefaultAction.runDefaultAction(defaultActionContext);
        return false;
    }
}
