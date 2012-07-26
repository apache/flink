/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.query;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.packages.IConstantRegistry;
import eu.stratosphere.sopremo.packages.IFunctionRegistry;

/**
 * @author Arvid Heise
 */
public class PackageManager implements ParsingScope {
	private Map<String, PackageInfo> packages = new HashMap<String, PackageInfo>();

	private String defaultJarPath;

	public final static IOperatorRegistry IORegistry = new DefaultOperatorRegistry();

	static {
		IORegistry.put(Sink.class);
		IORegistry.put(Source.class);
	}

	public PackageManager() {
		this.operatorRegistries.push(IORegistry);
	}

	private StackedConstantRegistry constantRegistries = new StackedConstantRegistry();

	private StackedFunctionRegistry functionRegistries = new StackedFunctionRegistry();

	private StackedOperatorRegistry operatorRegistries = new StackedOperatorRegistry();

	/**
	 * Imports sopremo-&lt;packageName&gt;.jar or returns a cached package structure.
	 * 
	 * @param packageName
	 */
	public PackageInfo getPackageInfo(String packageName) {
		PackageInfo packageInfo = this.packages.get(packageName);
		if (packageInfo == null) {
			File packagePath = this.getPackagePath(packageName);
			packageInfo = new PackageInfo(packageName);

			QueryUtil.LOG.debug("adding package " + packagePath);
			try {
				if (packagePath.getName().endsWith(".jar"))
					packageInfo.importFromJar(packagePath);
				else
					// should only happen while debugging
					packageInfo.importFromProject(packagePath.getAbsoluteFile());
			} catch (IOException e) {
				throw new IllegalArgumentException(String.format("could not load package %s", packagePath));
			}
			this.packages.put(packageName, packageInfo);
		}
		return packageInfo;
	}

	/**
	 * Returns the operatorFactory.
	 * 
	 * @return the operatorFactory
	 */
	@Override
	public IOperatorRegistry getOperatorRegistry() {
		return this.operatorRegistries;
	}

	@Override
	public IConstantRegistry getConstantRegistry() {
		return this.constantRegistries;
	}

	@Override
	public IFunctionRegistry getFunctionRegistry() {
		return this.functionRegistries;
	}

	protected File getPackagePath(String packageName) {
		String classpath = System.getProperty("java.class.path");
		String sopremoPackage = "sopremo-" + packageName;
		for (String path : classpath.split(File.pathSeparator)) {
			final int pathIndex = path.indexOf(sopremoPackage);
			if (pathIndex == -1)
				continue;
			// preceding character must be a file separator
			if (pathIndex > 0 && path.charAt(pathIndex - 1) != File.separatorChar)
				continue;
			int nextIndex = pathIndex + sopremoPackage.length();
			// next character must be '.' or file separator
			if (nextIndex < path.length() && path.charAt(nextIndex) != File.separatorChar
				&& path.charAt(nextIndex) != '.')
				continue;
			return new File(path);
		}
		final File defaultJar = new File(this.defaultJarPath, sopremoPackage + ".jar");
		if (defaultJar.exists())
			return defaultJar;
		throw new IllegalArgumentException(String.format("no package %s found", packageName));
	}

	public void importPackage(String packageName) {
		this.importPackage(this.getPackageInfo(packageName));
	}

	public void importPackage(PackageInfo packageInfo) {
		this.constantRegistries.push(packageInfo.getConstantRegistry());
		this.functionRegistries.push(packageInfo.getFunctionRegistry());
		this.operatorRegistries.push(packageInfo.getOperatorRegistry());
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("Package manager with packages %s", packages);
	}
}
