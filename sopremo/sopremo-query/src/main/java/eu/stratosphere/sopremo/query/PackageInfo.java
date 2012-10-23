/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
import java.util.LinkedList;
import java.util.Queue;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.packages.BuiltinProvider;
import eu.stratosphere.sopremo.packages.ConstantRegistryCallback;
import eu.stratosphere.sopremo.packages.DefaultConstantRegistry;
import eu.stratosphere.sopremo.packages.DefaultFunctionRegistry;
import eu.stratosphere.sopremo.packages.IConstantRegistry;
import eu.stratosphere.sopremo.packages.IFunctionRegistry;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * @author Arvid Heise
 */
public class PackageInfo implements ISerializableSopremoType, ParsingScope {
	/**
	 * 
	 */
	private static final long serialVersionUID = -253941926183824883L;

	/**
	 * Initializes PackageInfo.
	 * 
	 * @param packageName
	 * @param packagePath
	 */
	public PackageInfo(String packageName) {
		this.packageName = packageName;
	}

	private IOperatorRegistry operatorRegistry = new DefaultOperatorRegistry();

	private IConstantRegistry constantRegistry = new DefaultConstantRegistry();

	private IFunctionRegistry functionRegistry = new DefaultFunctionRegistry();

	private String packageName;

	private File packagePath;

	public String getPackageName() {
		return this.packageName;
	}

	public File getPackagePath() {
		return this.packagePath;
	}

	@SuppressWarnings("unchecked")
	private void importClass(String className) {
		Class<?> clazz;
		try {
			clazz = Class.forName(className);
			if (Operator.class.isAssignableFrom(clazz) && (clazz.getModifiers() & Modifier.ABSTRACT) == 0) {
				QueryUtil.LOG.trace("adding operator " + clazz);
				this.getOperatorRegistry().put((Class<? extends Operator<?>>) clazz);
			} else if (BuiltinProvider.class.isAssignableFrom(clazz))
				this.addFunctionsAndConstants(clazz);
		} catch (ClassNotFoundException e) {
			QueryUtil.LOG.warn("could not load operator " + className);
		}
	}

	public void importFromProject(File packagePath) {
		this.packagePath = packagePath;

		Queue<File> directories = new LinkedList<File>();
		directories.add(packagePath);
		while (!directories.isEmpty())
			for (File file : directories.poll().listFiles())
				if (file.isDirectory())
					directories.add(file);
				else if (file.getName().endsWith(".class") && !file.getName().contains("$"))
					this.importFromFile(file, packagePath);
	}

	private void importFromFile(File file, File packagePath) {
		String classFileName = file.getAbsolutePath().substring(packagePath.getAbsolutePath().length() + 1);
		String className = classFileName.replaceAll(".class$", "").replaceAll("/|\\\\", ".").replaceAll("^\\.", "");
		this.importClass(className);
	}

	private void addFunctionsAndConstants(Class<?> clazz) {
		this.getFunctionRegistry().put(clazz);
		if (ConstantRegistryCallback.class.isAssignableFrom(clazz))
			((ConstantRegistryCallback) ReflectUtil.newInstance(clazz)).registerConstants(this.getConstantRegistry());
	}

	public void importFromJar(File jar) throws IOException {
		this.packagePath = jar;
		Enumeration<JarEntry> entries = new JarFile(jar).entries();
		while (entries.hasMoreElements()) {
			JarEntry jarEntry = entries.nextElement();
			if (jarEntry.getName().endsWith(".class")) {
				String className =
					jarEntry.getName().replaceAll(".class$", "").replaceAll("/|\\\\", ".").replaceAll("^\\.", "");
				this.importClass(className);
			}
		}
	}

	@Override
	public void toString(StringBuilder builder) {
		builder.append("Package ").append(this.packageName);
		builder.append("\n  ");
		this.operatorRegistry.toString(builder);
		builder.append("\n  ");
		this.functionRegistry.toString(builder);
		builder.append("\n  ");
		this.constantRegistry.toString(builder);
	}

	@Override
	public IOperatorRegistry getOperatorRegistry() {
		return this.operatorRegistry;
	}

	@Override
	public IConstantRegistry getConstantRegistry() {
		return this.constantRegistry;
	}

	@Override
	public IFunctionRegistry getFunctionRegistry() {
		return this.functionRegistry;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return getPackageName();
	}
}
