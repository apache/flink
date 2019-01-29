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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * General test utilities.
 */
public enum TestUtils {
	;

	/**
	 * Searches for a jar matching the given regex in the given directory. This method is primarily intended to be used
	 * for the initialization of static {@link Path} fields for jars that reside in the modules {@code target} directory.
	 *
	 * <p>The given relative path is resolved against the {@code moduleDir} system property. For tests residing under
	 * {@code flink-end-to-end-tests} this value is set to the root directory of the module that contains the test.
	 *
	 * @param relativeDirectory directory path to search for, relative to the current working directory
	 * @param jarNameRegex regex pattern to match against
	 * @throws RuntimeException if none or multiple jars could be found
	 * @return Path pointing to the matching jar
	 */
	public static Path getResourceJar(final Path relativeDirectory, final String jarNameRegex) {
		final Path moduleDirectory = Paths.get(System.getProperty("moduleDir"));
		try (Stream<Path> dependencyJars = Files.list(moduleDirectory.resolve(relativeDirectory))) {
			final List<Path> matchingJars = dependencyJars
				.filter(jar -> Pattern.compile(jarNameRegex).matcher(jar.getFileName().toString()).matches())
				.collect(Collectors.toList());
			switch (matchingJars.size()) {
				case 0:
					throw new RuntimeException(new FileNotFoundException(String.format("No jar could be found that matches the pattern %s.", jarNameRegex)));
				case 1:
					return matchingJars.get(0);
				default:
					throw new RuntimeException(new IOException(String.format("Multiple jars were found matching the pattern %s. Matches=%s", jarNameRegex, matchingJars)));
			}
		} catch (final IOException ioe) {
			throw new RuntimeException("Could not search for resource jars.", ioe);
		}
	}
}
