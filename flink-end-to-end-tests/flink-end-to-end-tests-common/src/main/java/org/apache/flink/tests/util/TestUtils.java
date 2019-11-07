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
public class TestUtils {

	/**
	 * Searches for a jar matching the given regex in the given directory. This method is primarily intended to be used
	 * for the initialization of static {@link Path} fields for jars that reside in the modules {@code target} directory.
	 *
	 * @param jarNameRegex regex pattern to match against
	 * @return Path pointing to the matching jar
	 * @throws RuntimeException if none or multiple jars could be found
	 */
	public static Path getResourceJar(final String jarNameRegex) {
		String moduleDirProp = System.getProperty("moduleDir");
		Preconditions.checkNotNull(moduleDirProp);

		try (Stream<Path> dependencyJars = Files.walk(Paths.get(moduleDirProp))) {
			final List<Path> matchingJars = dependencyJars
				.filter(jar -> Pattern.compile(jarNameRegex).matcher(jar.toAbsolutePath().toString()).find())
				.collect(Collectors.toList());
			System.out.println(matchingJars);
			switch (matchingJars.size()) {
				case 0:
					throw new RuntimeException(
						new FileNotFoundException(
							String.format("No jar could be found that matches the pattern %s.", jarNameRegex)
						)
					);
				case 1:
					return matchingJars.get(0);
				default:
					throw new RuntimeException(
						new IOException(
							String.format("Multiple jars were found matching the pattern %s. Matches=%s", jarNameRegex, matchingJars)
						)
					);
			}
		} catch (final IOException ioe) {
			throw new RuntimeException("Could not search for resource jars.", ioe);
		}
	}
}
