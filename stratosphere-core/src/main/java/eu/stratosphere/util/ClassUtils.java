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

package eu.stratosphere.util;

import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.protocols.VersionedProtocol;

/**
 * Utility class which provides various methods for dynamic class loading.
 */
public final class ClassUtils {

	/**
	 * Private constructor used to overwrite public one.
	 */
	private ClassUtils() {}

	/**
	 * Searches for a protocol class by its name and attempts to load it.
	 * 
	 * @param className
	 *        the name of the protocol class
	 * @return an instance of the protocol class
	 * @throws ClassNotFoundException
	 *         thrown if no class with such a name can be found
	 */
	public static Class<? extends VersionedProtocol> getProtocolByName(final String className)
			throws ClassNotFoundException {

		if (!className.contains("Protocol")) {
			System.out.println(className);
			throw new ClassNotFoundException("Only use this method for protocols!");
		}

		return (Class<? extends VersionedProtocol>) Class.forName(className, true, getClassLoader()).asSubclass(VersionedProtocol.class);
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
	@SuppressWarnings("unchecked")
	public static Class<? extends IOReadableWritable> getRecordByName(final String className)
			throws ClassNotFoundException {
//		
//		Class<?> clazz = Class.forName(className, true, getClassLoader());
//		if (IOReadableWritable.class.isAssignableFrom(clazz)) {
//			return clazz.asSubclass(IOReadableWritable.class);
//		} else {
//			return (Class<? extends IOReadableWritable>) clazz;
//		}
//		
		return (Class<? extends IOReadableWritable>) Class.forName(className, true, getClassLoader());
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
	public static Class<? extends FileSystem> getFileSystemByName(final String className) throws ClassNotFoundException {
		return Class.forName(className, true, getClassLoader()).asSubclass(FileSystem.class);
	}


	private static ClassLoader getClassLoader() {
		return ClassUtils.class.getClassLoader();
	}
}
