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

package org.apache.flink.client.program;

import org.apache.flink.api.common.Plan;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.jar.JarFile;

/**
 * A JobWithJars is a Flink dataflow plan, together with a bunch of JAR files that contain
 * the classes of the functions and libraries necessary for the execution.
 */
public class JobWithJars {

	private Plan plan;

	private List<URL> jarFiles;

	/**
	 * classpaths that are needed during user code execution.
	 */
	private List<URL> classpaths;

	private ClassLoader userCodeClassLoader;

	public JobWithJars(Plan plan, List<URL> jarFiles, List<URL> classpaths) throws IOException {
		this.plan = plan;
		this.jarFiles = new ArrayList<URL>(jarFiles.size());
		this.classpaths = new ArrayList<URL>(classpaths.size());

		for (URL jarFile: jarFiles) {
			checkJarFile(jarFile);
			this.jarFiles.add(jarFile);
		}

		for (URL path: classpaths) {
			this.classpaths.add(path);
		}
	}

	public JobWithJars(Plan plan, URL jarFile) throws IOException {
		this.plan = plan;

		checkJarFile(jarFile);
		this.jarFiles = Collections.singletonList(jarFile);
		this.classpaths = Collections.<URL>emptyList();
	}

	JobWithJars(Plan plan, List<URL> jarFiles, List<URL> classpaths, ClassLoader userCodeClassLoader) {
		this.plan = plan;
		this.jarFiles = jarFiles;
		this.classpaths = classpaths;
		this.userCodeClassLoader = userCodeClassLoader;
	}

	/**
	 * Returns the plan.
	 */
	public Plan getPlan() {
		return this.plan;
	}

	/**
	 * Returns list of jar files that need to be submitted with the plan.
	 */
	public List<URL> getJarFiles() {
		return this.jarFiles;
	}

	/**
	 * Returns list of classpaths that need to be submitted with the plan.
	 */
	public List<URL> getClasspaths() {
		return classpaths;
	}

	/**
	 * Gets the {@link java.lang.ClassLoader} that must be used to load user code classes.
	 *
	 * @return The user code ClassLoader.
	 */
	public ClassLoader getUserCodeClassLoader() {
		if (this.userCodeClassLoader == null) {
			this.userCodeClassLoader = buildUserCodeClassLoader(jarFiles, classpaths, getClass().getClassLoader(), new Configuration());
		}
		return this.userCodeClassLoader;
	}

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

	/**
	 * @deprecated Use {@link #buildUserCodeClassLoader(List, List, ClassLoader, Configuration)}
	 */
	@Deprecated
	public static ClassLoader buildUserCodeClassLoader(List<URL> jars, List<URL> classpaths, ClassLoader parent) {
		return FlinkUserCodeClassLoaders.parentFirst(extractUrls(jars, classpaths), parent);
	}

	public static ClassLoader buildUserCodeClassLoader(List<URL> jars, List<URL> classpaths, ClassLoader parent, Configuration configuration) {
		URL[] urls = extractUrls(jars, classpaths);
		FlinkUserCodeClassLoaders.ResolveOrder resolveOrder = FlinkUserCodeClassLoaders.ResolveOrder
			.fromString(configuration.getString(CoreOptions.CLASSLOADER_RESOLVE_ORDER));
		String[] parentFirstPattern = CoreOptions.getParentFirstLoaderPatterns(configuration);
		return FlinkUserCodeClassLoaders.create(resolveOrder, urls, parent, parentFirstPattern);
	}

	private static URL[] extractUrls(List<URL> jars, List<URL> classpaths) {
		URL[] urls = new URL[jars.size() + classpaths.size()];
		for (int i = 0; i < jars.size(); i++) {
			urls[i] = jars.get(i);
		}
		for (int i = 0; i < classpaths.size(); i++) {
			urls[i + jars.size()] = classpaths.get(i);
		}
		return urls;
	}
}
