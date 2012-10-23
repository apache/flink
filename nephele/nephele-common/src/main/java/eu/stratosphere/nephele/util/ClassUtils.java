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

package eu.stratosphere.nephele.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.protocols.VersionedProtocol;
import eu.stratosphere.nephele.types.Record;

/**
 * Utility class which provides various methods for dynamic class loading.
 * 
 * @author warneke
 */
public final class ClassUtils {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(ClassUtils.class);

	/**
	 * Private constructor used to overwrite public one.
	 */
	private ClassUtils() {
	}

	/**
	 * Searches for a protocol class by its name and attempts to load it.
	 * 
	 * @param className
	 *        the name of the protocol class
	 * @return an instance of the protocol class
	 * @throws ClassNotFoundException
	 *         thrown if no class with such a name can be found
	 */
	// TODO: See if we can improve type safety here
	@SuppressWarnings("unchecked")
	public static Class<? extends VersionedProtocol> getProtocolByName(final String className)
			throws ClassNotFoundException {

		if (!className.contains("Protocol")) {
			throw new ClassNotFoundException("Only use this method for protocols!");
		}

		return (Class<? extends VersionedProtocol>) Class.forName(className, true, getClassLoader());
	}

	/**
	 * Searches for a record class by its name and attempts to load it.
	 * 
	 * @param className
	 *        the name of the record class
	 * @return an instance of the record class
	 * @throws ClassNotFoundException
	 *         thrown if no class with such a name can be found
	 */
	// TODO: See if we can improve type safety here
	@SuppressWarnings("unchecked")
	public static Class<? extends Record> getRecordByName(final String className) throws ClassNotFoundException {

		return (Class<? extends Record>) Class.forName(className, true, getClassLoader());

	}

	/**
	 * Searches for a file system class by its name and attempts to load it.
	 * 
	 * @param className
	 *        the name of the file system class
	 * @return an instance of the file system class
	 * @throws ClassNotFoundException
	 *         thrown if no class with such a name can be found
	 */
	// TODO: See if we can improve type safety here
	@SuppressWarnings("unchecked")
	public static Class<? extends FileSystem> getFileSystemByName(final String className) throws ClassNotFoundException {

		return (Class<? extends FileSystem>) Class.forName(className, true, getClassLoader());
	}

	/**
	 * Returns the thread's default class loader.
	 * 
	 * @return the thread's default class loader
	 */
	public static ClassLoader getClassLoader() {

		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		if (classLoader == null) {
			LOG.fatal("classLoader is null");
		}

		return classLoader;
	}
}
