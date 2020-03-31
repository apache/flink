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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Parser factory which generates a {@link PythonDriverOptions} from a given
 * list of command line arguments.
 */
final class PythonDriverOptionsParserFactory implements ParserResultFactory<PythonDriverOptions> {

	private static final Option PY_OPTION = Option.builder("py")
		.longOpt("python")
		.required(false)
		.hasArg(true)
		.argName("entrypoint python file")
		.desc("Python script with the program entry point. " +
			"The dependent resources can be configured with the `--pyFiles` option.")
		.build();

	private static final Option PYMODULE_OPTION = Option.builder("pym")
		.longOpt("pyModule")
		.required(false)
		.hasArg(true)
		.argName("entrypoint module name")
		.desc("Python module with the program entry point. " +
			"This option must be used in conjunction with `--pyFiles`.")
		.build();

	private static final Option PYFILES_OPTION = Option.builder("pyfs")
		.longOpt("pyFiles")
		.required(false)
		.hasArg(true)
		.argName("entrypoint python file")
		.desc("Attach custom python files for job. " +
			"Comma can be used as the separator to specify multiple files. " +
			"The standard python resource file suffixes such as .py/.egg/.zip are all supported." +
			"(eg: --pyFiles file:///tmp/myresource.zip,hdfs:///$namenode_address/myresource2.zip)")
		.build();

	@Override
	public Options getOptions() {
		final Options options = new Options();
		options.addOption(PY_OPTION);
		options.addOption(PYMODULE_OPTION);
		options.addOption(PYFILES_OPTION);
		return options;
	}

	@Override
	public PythonDriverOptions createResult(@Nonnull CommandLine commandLine) throws FlinkParseException {
		String entrypointModule = null;
		final List<Path> pythonLibFiles = new ArrayList<>();

		if (commandLine.hasOption(PY_OPTION.getOpt()) && commandLine.hasOption(PYMODULE_OPTION.getOpt())) {
			throw new FlinkParseException("Cannot use options -py and -pym simultaneously.");
		} else if (commandLine.hasOption(PY_OPTION.getOpt())) {
			Path file = new Path(commandLine.getOptionValue(PY_OPTION.getOpt()));
			String fileName = file.getName();
			if (fileName.endsWith(".py")) {
				entrypointModule = fileName.substring(0, fileName.length() - 3);
				pythonLibFiles.add(file);
			}
		} else if (commandLine.hasOption(PYMODULE_OPTION.getOpt())) {
			entrypointModule = commandLine.getOptionValue(PYMODULE_OPTION.getOpt());
		} else {
			throw new FlinkParseException(
				"The Python entrypoint has not been specified. It can be specified with options -py or -pym");
		}

		if (commandLine.hasOption(PYFILES_OPTION.getOpt())) {
			pythonLibFiles.addAll(
				Arrays.stream(commandLine.getOptionValue(PYFILES_OPTION.getOpt()).split(","))
					.map(Path::new)
					.collect(Collectors.toCollection(ArrayList::new)));
		} else if (commandLine.hasOption(PYMODULE_OPTION.getOpt())) {
			throw new FlinkParseException("Option -pym must be used in conjunction with `--pyFiles`");
		}

		return new PythonDriverOptions(entrypointModule, pythonLibFiles, commandLine.getArgList());
	}
}
