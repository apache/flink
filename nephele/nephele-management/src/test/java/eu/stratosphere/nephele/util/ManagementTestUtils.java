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

package eu.stratosphere.nephele.util;

import java.io.File;

import com.esotericsoftware.kryo.Kryo;

/**
 * This class contains auxiliary methods for unit tests in the Nephele management module.
 * 
 * @author warneke
 */
public final class ManagementTestUtils {

	/**
	 * The system property key to retrieve the user directory.
	 */
	private static final String USER_DIR_KEY = "user.dir";

	/**
	 * The directory the configuration directory is expected in when test are executed using Eclipse.
	 */
	private static final String ECLIPSE_PATH_EXTENSION = "/src/test/resources";

	/**
	 * Private constructor, so class cannot be instantiated.
	 */
	private ManagementTestUtils() {
	}

	/**
	 * Creates a copy of the given object by an in-memory serialization and subsequent
	 * deserialization.
	 * 
	 * @param original
	 *        the original object to be copied
	 * @return the copy of original object
	 */
	public static <T> T createCopy(final T original) {

		final Kryo kryo = new Kryo();
		return (T) kryo.copy(original);
	}

	/**
	 * Locates a file-based resource that is used during testing. The method makes sure that the resource is always
	 * located correctly, no matter if the test is executed in maven or Eclipse.
	 * 
	 * @param resourceName
	 *        the name of the resource to be located
	 * @return a file object pointing to the resource or <code>null</code> if the resource could not be located
	 */
	public static File locateResource(final String resourceName) {

		// This is the correct path for Maven-based tests
		File file = new File(System.getProperty(USER_DIR_KEY) + File.separator + resourceName);
		if (file.exists()) {
			return file;
		}

		file = new File(System.getProperty(USER_DIR_KEY) + ECLIPSE_PATH_EXTENSION + File.separator + resourceName);
		if (file.exists()) {
			return file;
		}

		return null;
	}
}
