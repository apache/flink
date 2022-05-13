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

package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;

import static org.apache.flink.client.cli.CliFrontendParser.ALL_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.RUNNING_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.SCHEDULED_OPTION;

/** Command line options for the LIST command. */
public class ListOptions extends CommandLineOptions {

    private final boolean showRunning;
    private final boolean showScheduled;
    private final boolean showAll;

    public ListOptions(CommandLine line) {
        super(line);
        this.showAll = line.hasOption(ALL_OPTION.getOpt());
        this.showRunning = line.hasOption(RUNNING_OPTION.getOpt());
        this.showScheduled = line.hasOption(SCHEDULED_OPTION.getOpt());
    }

    public boolean showRunning() {
        return showRunning;
    }

    public boolean showScheduled() {
        return showScheduled;
    }

    public boolean showAll() {
        return showAll;
    }
}
