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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for setting up command-line tool usages in a readable fashion.
 */
public enum CommandLineWrapper {
	;

	public static WGetBuilder wget(String url) {
		return new WGetBuilder(url);
	}

	/**
	 * Wrapper around wget used for downloading files.
	 */
	public static final class WGetBuilder {

		private final String url;
		private Path targetDir;

		WGetBuilder(String url) {
			this.url = url;
		}

		public WGetBuilder targetDir(Path dir) {
			this.targetDir = dir;
			return this;
		}

		public String[] build() {
			final List<String> commandsList = new ArrayList<>(5);
			commandsList.add("wget");
			commandsList.add("-q"); // silent
			//commandsList.add("--show-progress"); // enable progress bar
			if (targetDir != null) {
				commandsList.add("-P");
				commandsList.add(targetDir.toAbsolutePath().toString());
			}
			commandsList.add(url);
			return commandsList.toArray(new String[commandsList.size()]);
		}
	}

	public static SedBuilder sed(final String command, final Path file) {
		return new SedBuilder(command, file);
	}

	/**
	 * Wrapper around sed used for processing text.
	 */
	public static final class SedBuilder {

		private final String command;
		private final Path file;

		private boolean inPlace = false;

		SedBuilder(final String command, final Path file) {
			this.command = command;
			this.file = file;
		}

		public SedBuilder inPlace() {
			inPlace = true;
			return this;
		}

		public String[] build() {
			final List<String> commandsList = new ArrayList<>(5);
			commandsList.add("sed");
			if (inPlace) {
				commandsList.add("-i");
			}
			commandsList.add("-e");
			commandsList.add(command);
			commandsList.add(file.toAbsolutePath().toString());
			return commandsList.toArray(new String[commandsList.size()]);
		}
	}

	public static TarBuilder tar(final Path file) {
		return new TarBuilder(file);
	}

	/**
	 * Wrapper around tar used for extracting .tar archives.
	 */
	public static final class TarBuilder {

		private final Path file;
		private boolean zipped = false;
		private boolean extract = false;
		private Path targetDir;

		public TarBuilder(final Path file) {
			this.file = file;
		}

		public TarBuilder zipped() {
			zipped = true;
			return this;
		}

		public TarBuilder extract() {
			extract = true;
			return this;
		}

		public TarBuilder targetDir(final Path dir) {
			targetDir = dir;
			return this;
		}

		public String[] build() {
			final List<String> commandsList = new ArrayList<>(4);
			commandsList.add("tar");
			if (zipped) {
				commandsList.add("-z");
			}
			if (extract) {
				commandsList.add("-x");
			}
			if (targetDir != null) {
				commandsList.add("--directory");
				commandsList.add(targetDir.toAbsolutePath().toString());
			}
			commandsList.add("-f");
			commandsList.add(file.toAbsolutePath().toString());
			return commandsList.toArray(new String[commandsList.size()]);
		}
	}
}
