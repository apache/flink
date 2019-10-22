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

import static org.apache.flink.client.cli.CliFrontendParser.STOP_AND_DRAIN;
import static org.apache.flink.client.cli.CliFrontendParser.STOP_WITH_SAVEPOINT_PATH;

/**
 * Command line options for the STOP command.
 */
class StopOptions extends CommandLineOptions {

	private final String[] args;

	private final boolean savepointFlag;

	/** Optional target directory for the savepoint. Overwrites cluster default. */
	private final String targetDirectory;

	private final boolean advanceToEndOfEventTime;

	StopOptions(CommandLine line) {
		super(line);
		this.args = line.getArgs();

		this.savepointFlag = line.hasOption(STOP_WITH_SAVEPOINT_PATH.getOpt());
		this.targetDirectory = line.getOptionValue(STOP_WITH_SAVEPOINT_PATH.getOpt());

		this.advanceToEndOfEventTime = line.hasOption(STOP_AND_DRAIN.getOpt());
	}

	String[] getArgs() {
		return args == null ? new String[0] : args;
	}

	boolean hasSavepointFlag() {
		return savepointFlag;
	}

	String getTargetDirectory() {
		return targetDirectory;
	}

	boolean shouldAdvanceToEndOfEventTime() {
		return advanceToEndOfEventTime;
	}
}
