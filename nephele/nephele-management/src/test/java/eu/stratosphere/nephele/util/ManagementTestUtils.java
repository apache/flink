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

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;

/**
 * This class contains auxiliary methods for unit tests in the Nephele management module.
 * 
 * @author warneke
 */
public class ManagementTestUtils {

	/**
	 * Creates a copy of the given {@link IOReadableWritable} object by an in-memory serialization and subsequent
	 * deserialization.
	 * 
	 * @param original
	 *        the original object to be copied
	 * @return the copy of original object created by the original object's serialization/deserialization methods
	 */
	@SuppressWarnings("unchecked")
	public static IOReadableWritable createCopy(IOReadableWritable original) {

		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final DataOutputStream dos = new DataOutputStream(baos);

		try {
			original.write(dos);
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}

		final String className = original.getClass().getName();
		if (className == null) {
			fail("Class name is null");
		}

		Class<? extends IOReadableWritable> clazz = null;

		try {
			clazz = (Class<? extends IOReadableWritable>) Class.forName(className);
		} catch (ClassNotFoundException e) {
			fail(e.getMessage());
		}

		if (clazz == null) {
			fail("Cannot find class with name " + className);
		}

		IOReadableWritable copy = null;
		try {
			copy = clazz.newInstance();
		} catch (InstantiationException e) {
			fail(e.getMessage());
		} catch (IllegalAccessException e) {
			fail(e.getMessage());
		}

		if (copy == null) {
			fail("Copy of object of type " + className + " is null");
		}

		final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		final DataInputStream dis = new DataInputStream(bais);

		try {
			copy.read(dis);
		} catch (IOException e) {
			fail(e.getMessage());
		}

		return copy;
	}
}
