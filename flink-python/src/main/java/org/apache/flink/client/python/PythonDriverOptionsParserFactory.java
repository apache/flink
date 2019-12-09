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

import org.apache.flink.api.java.tuple.Tuple2;
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
			"These files will be added to the PYTHONPATH of both the local client and the remote python UDF worker. " +
			"The standard python resource file suffixes such as .py/.egg/.zip or directory are all supported. " +
			"Comma (',') could be used as the separator to specify multiple files " +
			"(e.g.: --pyFiles file:///tmp/myresource.zip,hdfs:///$namenode_address/myresource2.zip).")
		.build();

	private static final Option PYREQUIREMENTS_OPTION = Option.builder("pyreq")
		.longOpt("pyRequirements")
		.required(false)
		.hasArg(true)
		.argName("requirementsFilePath>#<requirementsCachedDir(optional)")
		.desc("Specify a requirements.txt file which defines the third-party dependencies. " +
			"These dependencies will be installed and added to the PYTHONPATH of the python UDF worker. " +
			"A directory which contains the installation packages of these dependencies could be specified " +
			"optionally. Use '#' as the separator if the optional parameter exists " +
			"(e.g.: --pyRequirements file:///tmp/requirements.txt#file:///tmp/cached_dir).")
		.build();

	private static final Option PYARCHIVE_OPTION = Option.builder("pyarch")
		.longOpt("pyArchives")
		.required(false)
		.hasArg(true)
		.argName("archiveFilePath>#<targetDirName(optional)>,...,<archiveFilePath>#<<targetDirName(optional)")
		.desc("Add python archive files for job. The archive files will be extracted to the working directory " +
			"of python UDF worker. Currently only zip-format is supported. For each archive file, a target directory " +
			"be specified. If the target directory name is specified, the archive file will be extracted to a " +
			"name can directory with the specified name. Otherwise, the archive file will be extracted to a " +
			"directory with the same name of the archive file. The files uploaded via this option are accessible " +
			"via relative path. '#' could be used as the separator of the archive file path and the target directory " +
			"name. Comma (',') could be used as the separator to specify multiple archive files. " +
			"This option can be used to upload the virtual environment, the data files used in Python UDF " +
			"(e.g.: --pyArchives file:///tmp/py37.zip,file:///tmp/data.zip#data --pyExecutable " +
			"py37.zip/py37/bin/python). The data files could be accessed in Python UDF, e.g.: " +
			"f = open('data/data.txt', 'r').")
		.build();

	private static final Option PYEXEC_OPTION = Option.builder("pyexec")
		.longOpt("pyExecutable")
		.required(false)
		.hasArg(true)
		.argName("pythonInterpreterPath")
		.desc("Specify the path of the python interpreter used to execute the python UDF worker " +
			"(e.g.: --pyExecutable /usr/local/bin/python3). " +
			"The python UDF worker depends on Python 3.5+, Apache Beam (version == 2.15.0), " +
			"Pip (version >= 7.1.0) and SetupTools (version >= 37.0.0). " +
			"Please ensure that the specified environment meets the above requirements.")
		.build();

	@Override
	public Options getOptions() {
		final Options options = new Options();
		options.addOption(PY_OPTION);
		options.addOption(PYMODULE_OPTION);
		options.addOption(PYFILES_OPTION);
		options.addOption(PYREQUIREMENTS_OPTION);
		options.addOption(PYARCHIVE_OPTION);
		options.addOption(PYEXEC_OPTION);
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

		List<String> pyFiles = new ArrayList<>();

		if (commandLine.hasOption(PYFILES_OPTION.getOpt())) {
			ArrayList<Path> pythonFileList =
				Arrays.stream(commandLine.getOptionValue(PYFILES_OPTION.getOpt()).split(","))
				.map(Path::new)
				.collect(Collectors.toCollection(ArrayList::new));
			pythonFileList.forEach(path -> pyFiles.add(path.getPath()));
			pythonLibFiles.addAll(pythonFileList);
		} else if (commandLine.hasOption(PYMODULE_OPTION.getOpt())) {
			throw new FlinkParseException("Option -pym must be used in conjunction with `--pyFiles`");
		}

		Tuple2<String, String> pyRequirements = null;
		if (commandLine.hasOption(PYREQUIREMENTS_OPTION.getOpt())) {
			String[] optionValues = commandLine.getOptionValue(PYREQUIREMENTS_OPTION.getOpt()).split("#");
			pyRequirements = new Tuple2<>();
			pyRequirements.f0 = optionValues[0];
			if (optionValues.length > 1) {
				pyRequirements.f1 = optionValues[1];
			}
		}

		List<Tuple2<String, String>> pyArchives = new ArrayList<>();
		if (commandLine.hasOption(PYARCHIVE_OPTION.getOpt())) {
			String[] archiveEntries = commandLine.getOptionValue(PYARCHIVE_OPTION.getOpt()).split(",");
			for (String archiveEntry : archiveEntries) {
				String[] pathAndTargetDir = archiveEntry.split("#");
				if (pathAndTargetDir.length > 1) {
					pyArchives.add(new Tuple2<>(pathAndTargetDir[0], pathAndTargetDir[1]));
				} else {
					pyArchives.add(new Tuple2<>(pathAndTargetDir[0], null));
				}
			}
		}

		String pyExecutable = null;
		if (commandLine.hasOption(PYEXEC_OPTION.getOpt())) {
			pyExecutable = commandLine.getOptionValue(PYEXEC_OPTION.getOpt());
		}

		return new PythonDriverOptions(
			entrypointModule,
			pythonLibFiles,
			commandLine.getArgList(),
			pyFiles,
			pyRequirements,
			pyExecutable,
			pyArchives);
	}
}
