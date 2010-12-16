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

package eu.stratosphere.nephele.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This abstract class must be implemented by every class whose objects have to be serialized
 * to their binary representation. Derived classes can be instantiated with java reflections
 * by calling the default constructor.
 * 
 * @author stanik
 */
public abstract class IOInstanceReadableWritable implements IOReadableWritable {

	/**
	 * Load a class and create an instance.
	 * 
	 * @param in
	 *        The data input.
	 * @return Returns an instance of IOInstanceReadableWritable.
	 */
	public static IOInstanceReadableWritable loadInstance(DataInput in) throws IOException {
		IOInstanceReadableWritable instance = null;

		// Read the name of the instance class name
		String instanceClassName = StringRecord.readString(in);

		if (instanceClassName == null) {
			throw new IOException("IOInstanceReadableWritable instanceClassName is null");
		}

		// Now load the instance from the given class name
		try {
			Class<?> classToLoad = Class.forName(instanceClassName);
			Object myObject = classToLoad.newInstance();
			if (myObject instanceof IOInstanceReadableWritable)
				instance = (IOInstanceReadableWritable) myObject;
			else
				throw new IOException("instance is not of type IOInstanceReadableWritable or null");
		} catch (ClassNotFoundException e) {
			throw new IOException("Cannot find class " + instanceClassName + ": " + StringUtils.stringifyException(e));
		} catch (InstantiationException e) {
			throw new IOException("Cannot instantiate class " + instanceClassName + ": "
				+ StringUtils.stringifyException(e));
		} catch (IllegalAccessException e) {
			throw new IOException("Cannot access class or method in class " + instanceClassName + ": "
				+ StringUtils.stringifyException(e));
		}

		instance.readInstance(in);
		return instance;
	}

	/**
	 * Read configuration from a data input.
	 * 
	 * @param in
	 *        The data input.
	 */
	public void readInstance(DataInput in) throws IOException {
		this.read(in);
	}

	/**
	 * Write instance type and configuration to a data output.
	 * 
	 * @param out
	 *        The data output.
	 * @param instance
	 *        The instance to write.
	 */
	public static void writeInstance(DataOutput out, IOInstanceReadableWritable instance) throws IOException {
		// Write out the name of the class
		if (instance == null) {
			throw new IOException("IOInstanceReadableWritable instance is null");
		}

		StringRecord.writeString(out, instance.getClass().getName());
		instance.write(out);
	}

	/**
	 * Write this instance type and configuration to a data output.
	 * 
	 * @param out
	 *        The data output.
	 */
	public void writeInstance(DataOutput out) throws IOException {
		writeInstance(out, this);
	}

}
