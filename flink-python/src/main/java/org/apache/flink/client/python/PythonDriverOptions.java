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

import javax.annotation.Nonnull;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Options for the {@link PythonDriver}.
 */
final class PythonDriverOptions {

	@Nonnull
	private String entrypointModule;

	@Nonnull
	private List<Path> pythonLibFiles;

	@Nonnull
	private List<String> programArgs;

	PythonDriverOptions(String entrypointModule, List<Path> pythonLibFiles, List<String> programArgs) {
		this.entrypointModule = requireNonNull(entrypointModule, "entrypointModule");
		this.pythonLibFiles = requireNonNull(pythonLibFiles, "pythonLibFiles");
		this.programArgs = requireNonNull(programArgs, "programArgs");
	}

	@Nonnull
	String getEntrypointModule() {
		return entrypointModule;
	}

	@Nonnull
	List<Path> getPythonLibFiles() {
		return pythonLibFiles;
	}

	@Nonnull
	List<String> getProgramArgs() {
		return programArgs;
	}
}
