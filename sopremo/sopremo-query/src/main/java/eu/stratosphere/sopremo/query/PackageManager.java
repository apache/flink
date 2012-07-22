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
import eu.stratosphere.sopremo.packages.DefaultConstantRegistry;
import eu.stratosphere.sopremo.packages.IConstantRegistry;
import eu.stratosphere.sopremo.packages.IMethodRegistry;

/**
 * @author Arvid Heise
 */
public class PackageManager {
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
	private StackedMethodRegistry methodRegistries = new StackedMethodRegistry();
	private StackedOperatorRegistry operatorRegistries = new StackedOperatorRegistry();

	/**
	 * Imports sopremo-&lt;packageName&gt;.jar or returns a cached package structure.
	 * 
	 * @param packageName
	 */
	public PackageInfo getPackageInfo(String packageName) {
		PackageInfo packageInfo = packages.get(packageName);
		if (packageInfo == null) {
			File packagePath = this.getPackagePath(packageName);
			packageInfo = new PackageInfo(packageName);

			QueryUtil.LOG.debug("adding package " + packagePath);
			try {
				if (packagePath.getName().endsWith(".jar"))
					packageInfo.importFromJar(packagePath);
				else
					// should only happen while debugging
					packageInfo.importFromProject(packagePath);
			} catch (IOException e) {
				throw new IllegalArgumentException(String.format("could not load package %s", packagePath));
			}
			packages.put(packageName, packageInfo);
		}
		return packageInfo;
	}

	/**
	 * Returns the operatorFactory.
	 * 
	 * @return the operatorFactory
	 */
	public IOperatorRegistry getOperatorRegistry() {
		return this.operatorRegistries;
	}
	
	public IConstantRegistry getConstantRegistry() {
		return this.constantRegistries;
	}
	
	public IMethodRegistry getMethodRegistry() {
		return this.methodRegistries;
	}

	protected File getPackagePath(String packageName) {
		String classpath = System.getProperty("java.class.path");
		String sopremoPackage = "sopremo-" + packageName;
		for (String path : classpath.split(File.pathSeparator))
			if (path.startsWith(sopremoPackage))
				return new File(path);
		final File defaultJar = new File(defaultJarPath, sopremoPackage + ".jar");
		if (defaultJar.exists())
			return defaultJar;
		throw new IllegalArgumentException(String.format("no package %s found", sopremoPackage));
	}

	public void importPackage(String packageName) {
		importPackage(getPackageInfo(packageName));
	}

	public void importPackage(PackageInfo packageInfo) {
		this.constantRegistries.push(packageInfo.getConstantRegistry());
		this.methodRegistries.push(packageInfo.getMethodRegistry());
		this.operatorRegistries.push(packageInfo.getOperatorRegistry());
	}

}
