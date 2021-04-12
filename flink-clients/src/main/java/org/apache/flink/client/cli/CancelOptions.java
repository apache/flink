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

import static org.apache.flink.client.cli.CliFrontendParser.CANCEL_WITH_SAVEPOINT_OPTION;

/** Command line options for the CANCEL command. */
public class CancelOptions extends CommandLineOptions {

    private final String[] args;

    /** Flag indicating whether to cancel with a savepoint. */
    private final boolean withSavepoint;

    /** Optional target directory for the savepoint. Overwrites cluster default. */
    private final String targetDirectory;

    public CancelOptions(CommandLine line) {
        super(line);
        this.args = line.getArgs();
        this.withSavepoint = line.hasOption(CANCEL_WITH_SAVEPOINT_OPTION.getOpt());
        this.targetDirectory = line.getOptionValue(CANCEL_WITH_SAVEPOINT_OPTION.getOpt());
    }

    public String[] getArgs() {
        return args == null ? new String[0] : args;
    }

    public boolean isWithSavepoint() {
        return withSavepoint;
    }

    public String getSavepointTargetDirectory() {
        return targetDirectory;
    }
}
