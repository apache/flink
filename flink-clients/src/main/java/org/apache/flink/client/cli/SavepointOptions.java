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
import org.apache.flink.api.common.JobID;
import org.apache.flink.util.StringUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.client.cli.CliFrontendParser.CLASSPATH_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.CLASS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.JAR_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.SAVEPOINT_DISPOSE_OPTION;

/**
 * Command line options for the SAVEPOINT command
 */
public class SavepointOptions extends CommandLineOptions {

	private final boolean dispose;
	private final String disposeSavepointPath;
	private final String entryPointClass;
	private final String jarFilePath;
	private final List<URL> classpaths;
	private final String[] programArgs;
	private final JobID jobId;

	public SavepointOptions(CommandLine line) throws CliArgsException {
		super(line);

		dispose = line.hasOption(SAVEPOINT_DISPOSE_OPTION.getOpt());
		String[] args = line.getArgs();

		// Dispose
		if (dispose) {
			disposeSavepointPath = line.getOptionValue(SAVEPOINT_DISPOSE_OPTION.getOpt());

			// Jar file
			jarFilePath = line.getOptionValue(JAR_OPTION.getOpt());

			// Either JAR file or job ID
			if (jarFilePath != null) {
				programArgs = args;
				jobId = null;
			} else {
				programArgs = null;

				if (args.length > 0) {
					try {
						String jobIdString = args[0];
						jobId = new JobID(StringUtils.hexStringToByte(jobIdString));
					} catch (Throwable t) {
						throw new CliArgsException("Job ID is not a valid ID");
					}
				} else {
					jobId = null;
				}
			}

			entryPointClass = line.getOptionValue(CLASS_OPTION.getOpt());

			// Class paths
			List<URL> classpaths = new ArrayList<>();
			if (line.hasOption(CLASSPATH_OPTION.getOpt())) {
				for (String path : line.getOptionValues(CLASSPATH_OPTION.getOpt())) {
					try {
						classpaths.add(new URL(path));
					} catch (MalformedURLException e) {
						throw new CliArgsException("Bad syntax for classpath: " + path);
					}
				}
			}
			this.classpaths = classpaths;
		} else {
			// Trigger
			disposeSavepointPath = null;
			jarFilePath = null;
			programArgs = null;
			entryPointClass = null;
			classpaths = Collections.emptyList();

			if (args.length > 0) {
				try {
					String jobIdString = args[0];
					jobId = new JobID(StringUtils.hexStringToByte(jobIdString));
				} catch (Throwable t) {
					throw new CliArgsException("Job ID is not a valid ID");
				}
			} else {
				jobId = null;
			}
		}
	}

	public boolean isDispose() {
		return dispose;
	}

	public String getSavepointPath() {
		return disposeSavepointPath;
	}

	public JobID getJobId() {
		return jobId;
	}

	public String[] getProgramArgs() {
		return programArgs;
	}

	public String getJarFilePath() {
		return jarFilePath;
	}

	public String getEntryPointClass() {
		return entryPointClass;
	}

	public List<URL> getClasspaths() {
		return classpaths;
	}

}
