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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URL;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

/**
 *  Abstract class for the JobGraphRetriever, which wants to get classpath user's code depends on.
 */
public abstract class AbstractUserClassPathJobGraphRetriever implements JobGraphRetriever {

	protected static final Logger LOG = LoggerFactory.getLogger(AbstractUserClassPathJobGraphRetriever.class);

	/** The directory contains all the jars, which user code depends on. */
	@Nullable
	private final String jobDir;

	private List<URL> userClassPaths;

	public AbstractUserClassPathJobGraphRetriever(String jobDir) {
		this.jobDir = jobDir;
	}

	public List<URL> getUserClassPaths() throws IOException {
		if (userClassPaths == null) {
			userClassPaths = getRelativeJarsURLFromDir(jobDir);
		}
		return userClassPaths;
	}

	/**
	 * Scan all the jar files in the {@code dir} and return all these jar files' relative URLs to "user.dir".
	 * @param dir the dir needed to scan the jar files
	 * @return the jar files' relative URLs
	 * @throws IOException
	 */
	private List<URL> getRelativeJarsURLFromDir(String dir) throws IOException {

		if (dir == null) {
			return Collections.emptyList();
		}

		final List<URL> jarURLs = new LinkedList<>();
		if (!Files.exists(Paths.get(dir))) {
			throw new IllegalArgumentException("the job dir " + dir + " dose not exists.");
		}
		if (!Files.isDirectory(Paths.get(dir))) {
			throw new IllegalArgumentException("the job dir " + dir + " is not a directory.");
		}

		Path dirPath;
		if (Paths.get(dir).isAbsolute()) {
			dirPath = Paths.get(System.getProperty("user.dir")).relativize(Paths.get(dir));
		} else {
			dirPath = Paths.get(dir);
		}
		Files.walkFileTree(
			dirPath,
			EnumSet.of(FileVisitOption.FOLLOW_LINKS),
			Integer.MAX_VALUE,
			new SimpleFileVisitor<java.nio.file.Path>() {

				@Override
				public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs)
						throws IOException {
					FileVisitResult fileVisitResult = super.visitFile(file, attrs);
					if (file.getFileName().toString().endsWith(".jar")) {
						LOG.info("add " + file.toString() + " to user classpath");
						jarURLs.add(
							new URL(new URL(file.getFileName().toUri().getScheme() + ":"), file.toString())
						);
					}
					return fileVisitResult;
				}
			});

		if (jarURLs.isEmpty()) {
			return Collections.emptyList();
		} else {
			return jarURLs;
		}
	}
}
