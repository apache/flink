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

package org.apache.flink.table.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Python execution environments.
 */
@Internal
public final class PythonEnv implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The path of the Python executable file used.
	 */
	private final String pythonExec;

	/**
	 * The command to start Python worker process.
	 */
	private final String pythonWorkerCmd;

	/**
	 * The execution type of the Python worker, it defines how to execute the Python functions.
	 */
	private final ExecType execType;

	public PythonEnv(
		String pythonExec,
		String pythonWorkerCmd,
		ExecType execType) {
		this.pythonExec = Preconditions.checkNotNull(pythonExec);
		this.pythonWorkerCmd = Preconditions.checkNotNull(pythonWorkerCmd);
		this.execType = Preconditions.checkNotNull(execType);
	}

	public String getPythonExec() {
		return pythonExec;
	}

	public String getPythonWorkerCmd() {
		return pythonWorkerCmd;
	}

	public ExecType getExecType() {
		return execType;
	}

	/**
	 * The Execution type specifies how to execute the Python function.
	 */
	public enum ExecType {
		// python function is executed in a separate process
		PROCESS
	}
}
