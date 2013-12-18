/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.client.program;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.api.Plan;

public class JobWithJars {
	
	private Plan plan;
	
	private List<File> jarFiles;
	
	private ClassLoader userCodeClassLoader;

	
	public JobWithJars(Plan plan, List<String> jarFiles) throws IOException {
		this.plan = plan;
		this.jarFiles = new ArrayList<File>(jarFiles.size());
		
		for (String jar: jarFiles) {
			File file = new File(jar);
			checkJarFile(file);
			this.jarFiles.add(file);
		}
	}
	
	public JobWithJars(Plan plan, String jarFile) throws IOException {
		this.plan = plan;
		
		File file = new File(jarFile);
		checkJarFile(file);
		this.jarFiles = Collections.singletonList(file);
	}
	
	JobWithJars(Plan plan, List<File> jarFiles, ClassLoader userCodeClassLoader) {
		this.plan = plan;
		this.jarFiles = jarFiles;
		this.userCodeClassLoader = userCodeClassLoader;
	}

	/**
	 * Returns the plan
	 */
	public Plan getPlan() {
		return this.plan;
	}

	/**
	 * Returns list of jar files that need to be submitted with the plan.
	 */
	public List<File> getJarFiles() throws IOException {
		return this.jarFiles;
	}
	
	/**
	 * Gets the {@link java.lang.ClassLoader} that must be used to load user code classes.
	 * 
	 * @return The user code ClassLoader.
	 */
	public ClassLoader getUserCodeClassLoader() {
		if (this.userCodeClassLoader == null) {
			this.userCodeClassLoader = buildUserCodeClassLoader(jarFiles, getClass().getClassLoader());
		}
		
		return this.userCodeClassLoader;
	}
	

	public static void checkJarFile(File jar) throws IOException {
		if (!jar.exists()) {
			throw new IOException("JAR file does not exist '" + jar.getAbsolutePath() + "'");
		}
		if (!jar.canRead()) {
			throw new IOException("JAR file can't be read '" + jar.getAbsolutePath() + "'");
		}
		// TODO: Check if proper JAR file
	}
	
	static ClassLoader buildUserCodeClassLoader(List<File> jars, ClassLoader parent) {
		
		URL[] urls = new URL[jars.size()];
		try {
			// add the nested jars
			for (int i = 0; i < jars.size(); i++) {
				urls[i] = jars.get(i).getAbsoluteFile().toURI().toURL();
			}
		}
		catch (MalformedURLException e) {
			// this should not happen, as all files should have been checked before for proper paths and existence.
			throw new RuntimeException("Cannot create class loader for program jar files: " + e.getMessage(), e);
		}
		
		return new URLClassLoader(urls, parent);
	}
}