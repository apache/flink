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

package org.apache.flink.runtime.operators.lifecycle.command;

import org.apache.flink.testutils.junit.SharedObjects;

import java.io.Serializable;

/**
 * A dispatcher for {@link TestCommand} executed by operators in a test job. The purpose of sending
 * commands is decoupling operator logic from control (i.e. test case logic).
 */
public interface TestCommandDispatcher extends Serializable {

    void subscribe(CommandExecutor executor, String operatorID);

    void dispatch(TestCommand command, TestCommandScope scope, String operatorID);

    void broadcast(TestCommand command, TestCommandScope scope);

    void unsubscribe(String operatorID, CommandExecutor commandExecutor);

    /** An executor of {@link TestCommand}s. */
    interface CommandExecutor {
        void execute(TestCommand testCommand);
    }

    /** Specifies whether to which subtasks to apply the command. */
    enum TestCommandScope {
        SINGLE_SUBTASK,
        ALL_SUBTASKS
    }

    static TestCommandDispatcher createShared(SharedObjects shared) {
        return new SharedTestCommandDispatcher(shared, new TestCommandDispatcherImpl());
    }
}
