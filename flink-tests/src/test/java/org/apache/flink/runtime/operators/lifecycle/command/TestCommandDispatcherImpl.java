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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.flink.util.Preconditions.checkState;

class TestCommandDispatcherImpl implements TestCommandDispatcher {
    private final Map<String, List<CommandExecutor>> subscribers = new ConcurrentHashMap<>();

    @Override
    public void subscribe(CommandExecutor executor, String operatorID) {
        subscribers.computeIfAbsent(operatorID, ign -> new CopyOnWriteArrayList<>()).add(executor);
    }

    @Override
    public void dispatch(TestCommand testCommand, TestCommandScope scope, String operatorID) {
        executeInternal(testCommand, scope, subscribers.getOrDefault(operatorID, emptyList()));
    }

    @Override
    public void broadcast(TestCommand testCommand, TestCommandScope scope) {
        executeInternal(
                testCommand,
                scope,
                subscribers.values().stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList()));
    }

    @Override
    public void unsubscribe(String operatorID, CommandExecutor commandExecutor) {
        subscribers.getOrDefault(operatorID, emptyList()).remove(commandExecutor);
    }

    private void executeInternal(
            TestCommand command, TestCommandScope scope, List<CommandExecutor> executors) {
        checkState(!executors.isEmpty(), "no executors for command: " + command);
        Set<CommandExecutor> toRemove = new HashSet<>();
        for (CommandExecutor executor : executors) {
            executor.execute(command);
            if (command.isTerminal()) {
                toRemove.add(executor);
            }
            if (scope == TestCommandScope.SINGLE_SUBTASK) {
                break;
            }
        }
        executors.removeAll(toRemove);
    }
}
