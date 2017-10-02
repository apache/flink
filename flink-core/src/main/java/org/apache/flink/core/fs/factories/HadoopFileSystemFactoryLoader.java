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

package org.apache.flink.core.fs.factories;

import org.apache.flink.core.fs.FileSystemFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class to check and reflectively load the Hadoop file system factory.
 */
public class HadoopFileSystemFactoryLoader {

	private static final Logger LOG = LoggerFactory.getLogger(HadoopFileSystemFactoryLoader.class);

	private static final String FACTORY_CLASS = "org.apache.flink.runtime.fs.hdfs.HadoopFsFactory";

	private static final String HADOOP_CONFIG_CLASS = "org.apache.hadoop.conf.Configuration";

	private static final String HADOOP_FS_CLASS = "org.apache.hadoop.fs.FileSystem";


	/**
	 * Loads the FileSystemFactory for the Hadoop-backed file systems.
	 */
	public static FileSystemFactory loadFactory() {
		final ClassLoader cl = HadoopFileSystemFactoryLoader.class.getClassLoader();

		// first, see if the Flink runtime classes are available
		final Class<? extends FileSystemFactory> factoryClass;
		try {
			factoryClass = Class.forName(FACTORY_CLASS, false, cl).asSubclass(FileSystemFactory.class);
		}
		catch (ClassNotFoundException e) {
			LOG.info("No Flink runtime dependency present - the extended set of supported File Systems " +
					"via Hadoop is not available.");
			return new UnsupportedSchemeFactory("Flink runtime classes missing in classpath/dependencies.");
		}
		catch (Exception | LinkageError e) {
			LOG.warn("Flink's Hadoop file system factory could not be loaded", e);
			return new UnsupportedSchemeFactory("Flink's Hadoop file system factory could not be loaded", e);
		}

		// check (for eager and better exception messages) if the Hadoop classes are available here
		try {
			Class.forName(HADOOP_CONFIG_CLASS, false, cl);
			Class.forName(HADOOP_FS_CLASS, false, cl);
		}
		catch (ClassNotFoundException e) {
			LOG.info("Hadoop is not in the classpath/dependencies - the extended set of supported File Systems " +
					"via Hadoop is not available.");
			return new UnsupportedSchemeFactory("Hadoop is not in the classpath/dependencies.");
		}

		// Create the factory.
		try {
			return factoryClass.newInstance();
		}
		catch (Exception | LinkageError e) {
			LOG.warn("Flink's Hadoop file system factory could not be created", e);
			return new UnsupportedSchemeFactory("Flink's Hadoop file system factory could not be created", e);
		}
	}
}
