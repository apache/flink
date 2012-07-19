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
import java.lang.reflect.Modifier;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.packages.BuiltinProvider;
import eu.stratosphere.sopremo.packages.ConstantRegistryCallback;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * @author Arvid Heise
 */
public class PackageManager {
	private Map<String, PackageInfo> packages = new HashMap<String, PackageInfo>();

	private String defaultJarPath;

	/**
	 * Imports sopremo-&lt;packageName&gt;.jar or returns a cached package structure.
	 * 
	 * @param packageName
	 */
	public PackageInfo getPackageInfo(String packageName) {
		final PackageInfo packageInfo = packages.get(packageName);
		if (packageInfo == null) {
			File packagePath = this.getPackagePath(packageName);
			packageInfo = new PackageInfo(packageName, packagePath);

			QueryUtil.LOG.debug("adding package " + packagePath);
			try {
				if (packagePath.getName().endsWith(".jar"))
					this.importFromJar(packageInfo);
				else
					// should only happen while debugging
					this.importFromProject(packageInfo);
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
	public OperatorRegistry getOperatorFactory() {
		return this.operatorFactory;
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

	private void importFromProject(PackageInfo info) {
		Queue<File> directories = new LinkedList<File>();
		directories.add(info.getPackagePath());
		while (!directories.isEmpty())
			for (File file : directories.poll().listFiles())
				if (file.isDirectory())
					directories.add(file);
				else if (file.getName().endsWith(".class") && !file.getName().contains("$"))
					this.importFromFile(info, file);
	}

	private void importFromFile(PackageInfo info, File file) {
		String classFileName = file.getName();
		String className = classFileName.replaceAll(".class$", "").replaceAll("/|\\\\", ".").replaceAll("^\\.", "");
		importClass(info, className);
	}

	@SuppressWarnings("unchecked")
	protected void importClass(PackageInfo info, String className) {
		Class<?> clazz;
		try {
			clazz = Class.forName(className);
			if (Operator.class.isAssignableFrom(clazz) && (clazz.getModifiers() & Modifier.ABSTRACT) == 0) {
				QueryUtil.LOG.trace("adding operator " + clazz);
				info.getOperatorRegistry().addOperator((Class<? extends Operator<?>>) clazz);
			} else if (BuiltinProvider.class.isAssignableFrom(clazz))
				this.addFunctionsAndConstants(info, clazz);
		} catch (ClassNotFoundException e) {
			QueryUtil.LOG.warn("could not load operator " + className);
		}
	}

	private void addFunctionsAndConstants(PackageInfo info, Class<?> clazz) {
		info.getMethodRegistry().register(clazz);
		if (ConstantRegistryCallback.class.isAssignableFrom(clazz))
			((ConstantRegistryCallback) ReflectUtil.newInstance(clazz)).registerConstants(this.getContext());
	}

	@SuppressWarnings("unused")
	private void importFromJar(String classPath, File file) throws IOException {
		Enumeration<JarEntry> entries = new JarFile(file).entries();
		while (entries.hasMoreElements()) {
			JarEntry jarEntry = entries.nextElement();
			if (jarEntry.getName().endsWith(".class")) {
				String className =
					jarEntry.getName().replaceAll(".class$", "").replaceAll("/|\\\\", ".").replaceAll("^\\.", "");
				importClass(className);
			}
		}
	}
}
