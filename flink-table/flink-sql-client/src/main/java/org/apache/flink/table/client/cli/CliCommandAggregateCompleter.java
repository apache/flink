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

package org.apache.flink.table.client.cli;

import org.apache.flink.table.client.config.SqlClientOptions;
import org.apache.flink.table.client.gateway.Executor;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Completer which contains multiple Flink SQL CLI command completers and aggregates them together.
 */
public class CliCommandAggregateCompleter extends AggregateCompleter {
    private final Map<CliStrings.SQLCliCommandsDescriptions, List<Completer>>
            command2dynamicCompleters = new EnumMap<>(CliStrings.SQLCliCommandsDescriptions.class);

    public CliCommandAggregateCompleter(String sessionId, Executor executor) {
        super(new ArrayList<>());
        initDynamicCompleter(sessionId, executor);
        List<Completer> completers = new ArrayList<>();

        for (CliStrings.SQLCliCommandsDescriptions command :
                CliStrings.SQLCliCommandsDescriptions.values()) {
            Set<String> names = new HashSet<>();
            names.add(command.getName());
            names.addAll(command.getOtherNames());
            for (String name : names) {
                List<Completer> compl = new ArrayList<>();
                for (String part : name.split("\\s+")) {
                    int semicolonPos = part.lastIndexOf(';');
                    compl.add(
                            new StringsCompleter(
                                    new FlinkCliCandidate(
                                            sessionId,
                                            executor,
                                            part,
                                            // for some command it makes sense to complete with ;
                                            // at the end however do not show it during completion
                                            // suggestion
                                            semicolonPos == part.length() - 1
                                                    ? part.substring(0, semicolonPos)
                                                    : part,
                                            null,
                                            command.getDescription(),
                                            null,
                                            // for equal command key should be same, e.g. QUIT and
                                            // EXIT
                                            name.indexOf(' ') == -1
                                                    ? command.getName()
                                                    : command.getName().replace(' ', '_')
                                                            + "_"
                                                            + part,
                                            true)));
                }

                if (command2dynamicCompleters.get(command) != null) {
                    compl.addAll(command2dynamicCompleters.get(command));
                }
                compl.add(new NullCompleter());
                completers.add(new ArgumentCompleter(compl));
            }
        }
        getCompleters().addAll(completers);
    }

    // In case completers require runtime info from executor then here it is a place to define them
    private void initDynamicCompleter(String sessionId, Executor executor) {
        command2dynamicCompleters.put(
                CliStrings.SQLCliCommandsDescriptions.RESET,
                Arrays.asList(
                        new PropertyKeyCompleter(sessionId, executor), new StringsCompleter("=")));
        command2dynamicCompleters.put(
                CliStrings.SQLCliCommandsDescriptions.SET,
                Arrays.asList(
                        new PropertyKeyCompleter(sessionId, executor), new StringsCompleter("=")));
    }

    /**
     * Completer candidate which shows/doesn't show description depending on {@code
     * SqlClientOptions.SHOW_COMPLETION_DESCRIPTION} option value.
     */
    public static class FlinkCliCandidate extends Candidate {
        private final Executor executor;
        private final String sessionId;

        FlinkCliCandidate(
                String sessionId,
                Executor executor,
                String value,
                String displ,
                String group,
                String descr,
                String suffix,
                String key,
                boolean complete) {
            super(value, displ, group, descr, suffix, key, complete);
            this.executor = executor;
            this.sessionId = sessionId;
        }

        @Override
        public String descr() {
            if (executor.getSessionConfig(sessionId)
                    .get(SqlClientOptions.SHOW_COMPLETION_DESCRIPTION)) {
                return super.descr();
            }
            return null;
        }
    }
}
