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
import java.util.LinkedList;
import java.util.Queue;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.packages.BuiltinProvider;
import eu.stratosphere.sopremo.packages.ConstantRegistryCallback;
import eu.stratosphere.sopremo.packages.DefaultConstantRegistry;
import eu.stratosphere.sopremo.packages.DefaultMethodRegistry;
import eu.stratosphere.sopremo.packages.IConstantRegistry;
import eu.stratosphere.sopremo.packages.IMethodRegistry;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * @author Arvid Heise
 */
public class PackageInfo implements ISerializableSopremoType {
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

	private IMethodRegistry methodRegistry = new DefaultMethodRegistry();

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
				getOperatorRegistry().put((Class<? extends Operator<?>>) clazz);
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
					this.importFromFile(file);
	}

	private void importFromFile(File file) {
		String classFileName = file.getName();
		String className = classFileName.replaceAll(".class$", "").replaceAll("/|\\\\", ".").replaceAll("^\\.", "");
		importClass(className);
	}

	private void addFunctionsAndConstants(Class<?> clazz) {
		getMethodRegistry().put(clazz);
		if (ConstantRegistryCallback.class.isAssignableFrom(clazz))
			((ConstantRegistryCallback) ReflectUtil.newInstance(clazz)).registerConstants(getConstantRegistry());
	}

	public void importFromJar(File jar) throws IOException {
		this.packagePath = jar;
		Enumeration<JarEntry> entries = new JarFile(jar).entries();
		while (entries.hasMoreElements()) {
			JarEntry jarEntry = entries.nextElement();
			if (jarEntry.getName().endsWith(".class")) {
				String className =
					jarEntry.getName().replaceAll(".class$", "").replaceAll("/|\\\\", ".").replaceAll("^\\.", "");
				importClass(className);
			}
		}
	}

	@Override
	public void toString(StringBuilder builder) {
		builder.append("Package ").append(packageName);
		builder.append("\n  ");
		operatorRegistry.toString(builder);
		builder.append("\n  ");
		methodRegistry.toString(builder);
		builder.append("\n  ");
		constantRegistry.toString(builder);
	}

	public IOperatorRegistry getOperatorRegistry() {
		return this.operatorRegistry;
	}

	public IConstantRegistry getConstantRegistry() {
		return this.constantRegistry;
	}

	public IMethodRegistry getMethodRegistry() {
		return this.methodRegistry;
	}

}
