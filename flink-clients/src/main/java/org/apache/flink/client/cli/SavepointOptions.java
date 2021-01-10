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

import static org.apache.flink.client.cli.CliFrontendParser.JAR_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.SAVEPOINT_DISPOSE_OPTION;

/** Command line options for the SAVEPOINT command. */
public class SavepointOptions extends CommandLineOptions {

    private final String[] args;
    private boolean dispose;
    private String disposeSavepointPath;
    private String jarFile;

    public SavepointOptions(CommandLine line) {
        super(line);
        args = line.getArgs();
        dispose = line.hasOption(SAVEPOINT_DISPOSE_OPTION.getOpt());
        disposeSavepointPath = line.getOptionValue(SAVEPOINT_DISPOSE_OPTION.getOpt());
        jarFile = line.getOptionValue(JAR_OPTION.getOpt());
    }

    public String[] getArgs() {
        return args == null ? new String[0] : args;
    }

    public boolean isDispose() {
        return dispose;
    }

    public String getSavepointPath() {
        return disposeSavepointPath;
    }

    public String getJarFilePath() {
        return jarFile;
    }
}
