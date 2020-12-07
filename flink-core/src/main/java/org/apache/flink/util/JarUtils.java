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

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

/**
 * Utility functions for jar files.
 */
@Internal
public class JarUtils {

	public static void checkJarFile(URL jar) throws IOException {
		File jarFile;
		try {
			jarFile = new File(jar.toURI());
		} catch (URISyntaxException e) {
			throw new IOException("JAR file path is invalid '" + jar + '\'');
		}
		if (!jarFile.exists()) {
			throw new IOException("JAR file does not exist '" + jarFile.getAbsolutePath() + '\'');
		}
		if (!jarFile.canRead()) {
			throw new IOException("JAR file can't be read '" + jarFile.getAbsolutePath() + '\'');
		}

		try (JarFile ignored = new JarFile(jarFile)) {
			// verify that we can open the Jar file
		} catch (IOException e) {
			throw new IOException("Error while opening jar file '" + jarFile.getAbsolutePath() + '\'', e);
		}
	}

	public static List<URL> getJarFiles(final String[] jars) {
		if (jars == null) {
			return Collections.emptyList();
		}

		return Arrays.stream(jars).map(jarPath -> {
			try {
				final URL fileURL = new File(jarPath).getAbsoluteFile().toURI().toURL();
				JarUtils.checkJarFile(fileURL);
				return fileURL;
			} catch (MalformedURLException e) {
				throw new IllegalArgumentException("JAR file path invalid", e);
			} catch (IOException e) {
				throw new RuntimeException("Problem with jar file " + jarPath, e);
			}
		}).collect(Collectors.toList());
	}
}
