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

package org.apache.flink.tests.util;

import org.apache.flink.util.Preconditions;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FlinkClient {

	public enum Action {
		RUN, INFO, LIST, STOP, CANCEL, SAVEPOINT
	}

	private final Path bin;
	private Action action;
	private boolean dettached;
	private String jobManagerAddress = null;
	private Path jarFile;
	private final List<String> extraArgList = new ArrayList<>();

	public FlinkClient(Path bin) {
		this.bin = bin;
	}

	public FlinkClient action(Action action) {
		this.action = action;
		return this;
	}

	public FlinkClient jobManagerAddress(String jobManagerAddress) {
		this.jobManagerAddress = jobManagerAddress;
		return this;
	}

	public FlinkClient dettached(boolean dettached) {
		this.dettached = dettached;
		return this;
	}

	public FlinkClient extraArgs(String[] extraArgs) {
		Collections.addAll(extraArgList, extraArgs);
		return this;
	}

	public FlinkClient jarFile(Path jar) {
		this.jarFile = jar;
		return this;
	}

	public AutoClosableProcess.AutoClosableProcessBuilder createProcess() {
		List<String> commands = new ArrayList<>();
		commands.add(bin.resolve("flink").toAbsolutePath().toString());
		Preconditions.checkNotNull(this.action);
		Preconditions.checkNotNull(this.jarFile);

		commands.add(this.action.toString().toLowerCase());

		// Make it in dettached mode if set.
		if (dettached) {
			commands.add("-d");
		}

		// Add the job manager address if set.
		if (this.jobManagerAddress != null) {
			commands.add("--jobmanager");
			commands.add(this.jobManagerAddress);
		}

		commands.add(this.jarFile.toAbsolutePath().toString());
		commands.addAll(extraArgList);
		return AutoClosableProcess.create(commands.toArray(new String[0]));
	}
}
