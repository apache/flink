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
import org.apache.flink.testutils.junit.SharedReference;

/**
 * {@link TestCommandDispatcher} implementation that delegates to another one wrapped into a {@link
 * SharedReference} to avoid serialization issues.
 */
class SharedTestCommandDispatcher implements TestCommandDispatcher {
    private final SharedReference<TestCommandDispatcher> ref;

    public SharedTestCommandDispatcher(SharedObjects shared, TestCommandDispatcher delegate) {
        this.ref = shared.add(delegate);
    }

    @Override
    public void dispatch(TestCommand testCommand, TestCommandScope scope, String operatorID) {
        ref.get().dispatch(testCommand, scope, operatorID);
    }

    @Override
    public void subscribe(CommandExecutor executor, String operatorID) {
        ref.get().subscribe(executor, operatorID);
    }

    @Override
    public void broadcast(TestCommand testCommand, TestCommandScope scope) {
        ref.get().broadcast(testCommand, scope);
    }
}
