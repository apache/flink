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

package org.apache.flink.client.python;

import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import javax.annotation.Nonnull;

import static org.apache.flink.client.cli.CliFrontendParser.PYMODULE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PY_OPTION;

/**
 * Parser factory which generates a {@link PythonDriverOptions} from a given list of command line
 * arguments.
 */
final class PythonDriverOptionsParserFactory implements ParserResultFactory<PythonDriverOptions> {

    @Override
    public Options getOptions() {
        final Options options = new Options();
        options.addOption(PY_OPTION);
        options.addOption(PYMODULE_OPTION);
        return options;
    }

    @Override
    public PythonDriverOptions createResult(@Nonnull CommandLine commandLine)
            throws FlinkParseException {
        String entryPointModule = null;
        String entryPointScript = null;

        if (commandLine.hasOption(PY_OPTION.getOpt())
                && commandLine.hasOption(PYMODULE_OPTION.getOpt())) {
            throw new FlinkParseException("Cannot use options -py and -pym simultaneously.");
        } else if (commandLine.hasOption(PY_OPTION.getOpt())) {
            entryPointScript = commandLine.getOptionValue(PY_OPTION.getOpt());
        } else if (commandLine.hasOption(PYMODULE_OPTION.getOpt())) {
            entryPointModule = commandLine.getOptionValue(PYMODULE_OPTION.getOpt());
        } else {
            throw new FlinkParseException(
                    "The Python entry point has not been specified. It can be specified with options -py or -pym");
        }

        return new PythonDriverOptions(
                entryPointModule, entryPointScript, commandLine.getArgList());
    }
}
