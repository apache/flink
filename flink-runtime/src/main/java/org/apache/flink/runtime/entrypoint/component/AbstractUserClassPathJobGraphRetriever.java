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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.util.FileUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 *  Abstract class for the JobGraphRetriever, which wants to get classpath user's code depends on.
 */
public abstract class AbstractUserClassPathJobGraphRetriever implements JobGraphRetriever {

	/* A collection of relative jar paths to the working directory */
	private final List<URL> userClassPaths;

	protected AbstractUserClassPathJobGraphRetriever(@Nullable final File jobDir) throws IOException {
		if (jobDir == null) {
			userClassPaths = Collections.emptyList();
		} else {
			final Collection<File> jarFiles = FileUtils.listFilesInPath(jobDir, file -> file.getName().endsWith(".jar"));
			final Collection<File> relativeFiles = FileUtils.relativizeToWorkingDir(jarFiles);
			this.userClassPaths = new ArrayList<>(FileUtils.toRelativeURLs(relativeFiles));
			if (this.userClassPaths.isEmpty()) {
				throw new IllegalArgumentException(String.format("The job dir %s does not have any jars.", jobDir));
			}
		}
	}

	public List<URL> getUserClassPaths() {
		return userClassPaths;
	}
}
